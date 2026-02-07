use FerroDB::aof::{AofWriter, load_aof};
use FerroDB::commands::handle_command;
use FerroDB::persistance::load_rdb;
use FerroDB::protocol::parse_resp;
use FerroDB::storage::FerroStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, interval};

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
            handle_command(cmd, &store_ref, None).await;
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
        tokio::spawn(async move {
            if let Err(e) = process_connection(socket, store_clone, aof_clone).await {
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
) -> Result<(), Box<dyn std::error::Error>> {
    // Create a buffer to accumulate data
    let mut buffer = Vec::new();
    let mut temp = [0u8; 1024];

    loop {
        // Read data from socket
        let n = socket.read(&mut temp).await?;

        if n == 0 {
            println!("Client disconnected");
            return Ok(());
        }

        // Append to our buffer
        buffer.extend_from_slice(&temp[..n]);

        // Try to parse complete RESP messages from buffer
        loop {
            let buffer_str = String::from_utf8_lossy(&buffer);

            // Try to find a complete RESP message
            match try_parse_complete_message(&buffer_str) {
                Some((message, consumed_bytes)) => {
                    println!("Received: {:?}", message);

                    // Parse and handle the command
                    match parse_resp(&message) {
                        Ok(parsed) => {
                            let response = handle_command(parsed, &store, Some(&aof)).await;
                            let encoded = response.encode();

                            socket.write_all(encoded.as_bytes()).await?;
                            println!("Sent: {:?}", encoded);
                        }
                        Err(e) => {
                            let err_msg = format!("-ERR parse error: {}\r\n", e);
                            socket.write_all(err_msg.as_bytes()).await?;
                        }
                    }

                    // Remove the consumed bytes from buffer
                    buffer.drain(..consumed_bytes);
                }
                None => {
                    // Not enough data for a complete message yet
                    break;
                }
            }
        }
    }
}

// TODO: Understand what this function does
// Helper: Check if we have a complete RESP message in the buffer
// Returns (message_string, bytes_consumed) if complete
fn try_parse_complete_message(input: &str) -> Option<(String, usize)> {
    let lines: Vec<&str> = input.split("\r\n").collect();
    if lines.is_empty() {
        return None;
    }

    let first_line = lines[0];
    if first_line.is_empty() {
        return None;
    }

    let prefix = first_line.chars().next()?;

    match prefix {
        '+' | '-' | ':' => {
            // Simple types: just one line
            if lines.len() >= 2 {
                let message = format!("{}\r\n", lines[0]);
                let consumed = message.len();
                Some((message, consumed))
            } else {
                None
            }
        }
        '$' => {
            // Bulk string: $<length>\r\n<data>\r\n
            if lines.len() < 3 {
                return None;
            }

            let len_str = &first_line[1..];
            let len: i64 = len_str.parse().ok()?;

            if len == -1 {
                // Null bulk string
                let message = format!("{}\r\n", lines[0]);
                let consumed = message.len();
                return Some((message, consumed));
            }

            // Check if we have the data
            let data_line = lines[1];
            let message = format!("{}\r\n{}\r\n", lines[0], data_line);
            let consumed = message.len();
            Some((message, consumed))
        }
        '*' => {
            // Array: need to count elements recursively
            // For now, use a heuristic: look for enough \r\n
            let count_str = &first_line[1..];
            let count: usize = count_str.parse().ok()?;

            // Simplified: estimate lines needed
            // Each array element needs at least 2 lines (type + data)
            let estimated_lines = 1 + count * 2;

            if lines.len() < estimated_lines {
                return None; // Not enough data yet
            }

            // Try to find the end of the array by tracking depth
            let mut idx = 0;

            if let Some(consumed) = count_array_lines(&lines, &mut idx) {
                let message_lines = &lines[..consumed];
                let message = message_lines.join("\r\n") + "\r\n";
                let len = message.len();
                Some((message, len))
            } else {
                None
            }
        }
        _ => None,
    }
}

// Recursively count lines needed for an array
fn count_array_lines(lines: &[&str], idx: &mut usize) -> Option<usize> {
    if *idx >= lines.len() {
        return None;
    }

    let line = lines[*idx];
    if line.is_empty() && *idx == lines.len() - 1 {
        // Last empty line from split
        return Some(*idx);
    }

    let prefix = line.chars().next()?;
    *idx += 1;

    match prefix {
        '*' => {
            let count_str = &line[1..];
            let count: usize = count_str.parse().ok()?;

            for _ in 0..count {
                count_array_lines(lines, idx)?;
            }
            Some(*idx)
        }
        '$' => {
            let len_str = &line[1..];
            let len: i64 = len_str.parse().ok()?;

            if len == -1 {
                Some(*idx)
            } else {
                *idx += 1; // Skip data line
                Some(*idx)
            }
        }
        '+' | '-' | ':' => Some(*idx),
        _ => None,
    }
}
