use FerroDB::aof::{AofWriter, load_aof};
use FerroDB::commands::handle_command;
use FerroDB::persistance::load_rdb;
use FerroDB::protocol::{RespValue, parse_resp};
use FerroDB::pubsub::{ClientSubscriptions, PubSubHub};
use FerroDB::storage::FerroStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, interval, sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = FerroStore::new();
    if let Err(e) = load_rdb(&store, "dump.rdb").await {
        println!("No existing database found or failed to load: {}", e);
        println!("Starting with empty database");
    } else {
        println!("Loaded {} keys from dump.rdb", store.dbsize());
    }
    let store_clone = store.clone();
    let commands_replayed = load_aof("appendonly.aof", move |cmd| {
        // Replay command without logging back to AOF
        let rt = tokio::runtime::Handle::current();
        let store_ref = store_clone.clone();
        rt.spawn(async move {
            handle_command(cmd, &store_ref, None, None, None).await;
        });
    })
    .await?;
    if commands_replayed > 0 {
        println!("Replayed {} commands from AOF", commands_replayed);
        println!("Total keys after AOF replay: {}", store.dbsize());
    }
    let (aof_writer, aof_handle) = AofWriter::new("appendonly.aof".to_string());
    tokio::spawn(async move {
        if let Err(e) = aof_handle.run().await {
            eprintln!("AOF writer error: {}", e);
        }
    });

    let pubsub = PubSubHub::new();

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("FerroDB listening on port 6379");
    let store_clone = store.clone();
    tokio::spawn(async move { active_expiration_loop(store_clone).await });
    // Periodic auto-save task (every 60 seconds)
    let store_clone = store.clone();
    tokio::spawn(async move {
        auto_save_loop(store_clone).await;
    });

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from: {}", addr);

        let store_clone = store.clone();
        let aof_clone = aof_writer.clone();
        let pubsubclone = pubsub.clone();
        tokio::spawn(async move {
            if let Err(e) = process_connection(socket, store_clone, aof_clone, pubsubclone).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn active_expiration_loop(store: FerroStore) {
    let mut ticker = interval(Duration::from_millis(100)); //Run every 100 ms
    loop {
        ticker.tick().await;
        let deleted = store.delete_expired_keys();
        if deleted > 0 {
            println!("Active expiration: deleted {} expired keys", deleted);
        }
    }
}
async fn auto_save_loop(store: FerroStore) {
    let mut ticker = interval(Duration::from_secs(60));

    loop {
        ticker.tick().await;

        if store.dbsize() > 0 {
            match FerroDB::persistance::save_rdb(&store, "dump.rdb").await {
                Ok(_) => println!("Auto-save: saved {} keys to dump.rdb", store.dbsize()),
                Err(e) => eprintln!("Auto-save failed: {}", e),
            }
        }
    }
}

async fn process_connection(
    mut socket: TcpStream,
    store: FerroStore,
    aof: AofWriter,
    pubsub: PubSubHub, // ✅ Add this
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let mut temp = [0u8; 1024];
    let mut client_subs = ClientSubscriptions::new(); // ✅ Add this

    loop {
        // Check for pub/sub messages if subscribed
        if client_subs.is_subscribed() {
            // Non-blocking check for messages
            while let Some(msg) = client_subs.try_recv() {
                // Send message to client
                // Format: ["message", channel, message_content]
                let response = RespValue::Array(vec![
                    RespValue::BulkString("message".to_string()),
                    RespValue::BulkString(msg.channel),
                    RespValue::BulkString(msg.message),
                ]);
                socket.write_all(response.encode().as_bytes()).await?;
            }
        }

        // Try to read from socket (with timeout if subscribed)
        let n = if client_subs.is_subscribed() {
            // Use timeout to periodically check for pub/sub messages
            tokio::select! {
                result = socket.read(&mut temp) => result?,
                _ = sleep(Duration::from_millis(100)) => {
                    // Timeout - continue to check for pub/sub messages
                    continue;
                }
            }
        } else {
            socket.read(&mut temp).await?
        };

        if n == 0 {
            println!("Client disconnected");
            return Ok(());
        }

        buffer.extend_from_slice(&temp[..n]);

        while let Some((msg, consumed)) = extract_message(&buffer) {
            println!("Received: {}", msg.escape_debug());

            match parse_resp(&msg) {
                Ok(parsed) => {
                    let response = handle_command(
                        parsed,
                        &store,
                        Some(&aof),
                        Some(&pubsub),
                        Some(&mut client_subs),
                    )
                    .await;
                    let encoded = response.encode();
                    socket.write_all(encoded.as_bytes()).await?;
                    println!("Sent: {}", encoded.escape_debug());
                }
                Err(e) => {
                    let err_msg = format!("-ERR {}\r\n", e);
                    socket.write_all(err_msg.as_bytes()).await?;
                }
            }

            buffer.drain(..consumed);
        }
    }
}
fn extract_message(buffer: &[u8]) -> Option<(String, usize)> {
    let s = String::from_utf8_lossy(buffer);
    let mut lines = s.split("\r\n");

    let first = lines.next()?.trim();
    if first.is_empty() {
        return None;
    }

    let prefix = first.chars().next()?;

    match prefix {
        '+' | '-' | ':' => {
            let msg = format!("{}\r\n", first);
            Some((msg.clone(), msg.len()))
        }
        '$' => {
            let len: i64 = first[1..].parse().ok()?;

            if len == -1 {
                let msg = "$-1\r\n".to_string();
                return Some((msg.clone(), msg.len()));
            }

            let data = lines.next()?;
            let msg = format!("{}\r\n{}\r\n", first, data);
            Some((msg.clone(), msg.len()))
        }
        '*' => parse_array_from_buffer(&s),
        _ => None,
    }
}
fn parse_array_from_buffer(input: &str) -> Option<(String, usize)> {
    let mut pos = 0;
    let bytes = input.as_bytes();

    let (first_line, line_end) = read_line(bytes, pos)?;
    pos = line_end;

    let count: usize = first_line.trim_start_matches('*').parse().ok()?;
    let mut result = first_line.to_string() + "\r\n";

    for _ in 0..count {
        let (element, elem_end) = parse_element_from_pos(bytes, pos)?;
        result.push_str(&element);
        pos = elem_end;
    }

    Some((result, pos))
}

fn parse_element_from_pos(bytes: &[u8], start: usize) -> Option<(String, usize)> {
    let mut pos = start;

    let (type_line, line_end) = read_line(bytes, pos)?;
    pos = line_end;

    let prefix = type_line.chars().next()?;

    match prefix {
        '+' | '-' | ':' => Some((format!("{}\r\n", type_line), pos)),
        '$' => {
            let len: i64 = type_line[1..].parse().ok()?;

            if len == -1 {
                return Some(("$-1\r\n".to_string(), pos));
            }

            let (data_line, data_end) = read_line(bytes, pos)?;
            pos = data_end;

            Some((format!("{}\r\n{}\r\n", type_line, data_line), pos))
        }
        '*' => {
            let count: usize = type_line[1..].parse().ok()?;
            let mut result = format!("{}\r\n", type_line);

            for _ in 0..count {
                let (elem, elem_end) = parse_element_from_pos(bytes, pos)?;
                result.push_str(&elem);
                pos = elem_end;
            }

            Some((result, pos))
        }
        _ => None,
    }
}

fn read_line(bytes: &[u8], start: usize) -> Option<(&str, usize)> {
    let remaining = &bytes[start..];

    for i in 0..remaining.len().saturating_sub(1) {
        if remaining[i] == b'\r' && remaining[i + 1] == b'\n' {
            let line = std::str::from_utf8(&remaining[..i]).ok()?;
            return Some((line, start + i + 2));
        }
    }

    None
}
