use crate::protocol::RespValue;
use std::io;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};
#[derive(Clone)]
pub struct AofWriter {
    sender: mpsc::UnboundedSender<String>,
}

pub struct AofHandle {
    receiver: mpsc::UnboundedReceiver<String>,
    path: String,
}

impl AofWriter {
    pub fn new(path: String) -> (Self, AofHandle) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let handle = AofHandle { receiver, path };
        (AofWriter { sender }, handle)
    }

    pub fn log_command(&self, command: &RespValue) {
        let encoded = command.encode();
        let _ = self.sender.send(encoded);
    }
}

impl AofHandle {
    pub async fn run(mut self) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        let mut buffer: Vec<String> = Vec::new();
        let mut sync_interval = interval(Duration::from_secs(1));

        loop {
            tokio::select! {

                Some(command) = self.receiver.recv() => {

                    buffer.push(command);
                }
                _=sync_interval.tick() => {
                    if !buffer.is_empty() {

                        for cmd in buffer.drain(..) {
                            file.write_all(cmd.as_bytes()).await?;
                        }
                        file.sync_data().await?;
                        println!("AOF Flushed and synced to disk");
                    }
                }
            }
        }
    }
}

pub async fn load_aof<F>(path: &str, mut replay_fn: F) -> io::Result<usize>
where
    F: FnMut(RespValue),
{
    let file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            println!("No AOF file found at {}", path);
            return Ok(0);
        }
        Err(e) => return Err(e),
    };
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut command_count = 0;
    let mut buffer = String::new();
    while let Some(line) = lines.next_line().await? {
        buffer.push_str(&line);
        buffer.push_str("\r\n");
        if let Ok(command) = crate::protocol::parse_resp(&buffer) {
            replay_fn(command);
            command_count += 1;
            buffer.clear();
        }
    }
    Ok(command_count)
}

pub async fn rewrite_aof(
    current_data: Vec<(
        String,
        crate::storage::DataType,
        Option<std::time::Duration>,
    )>,
    path: &str,
) -> io::Result<()> {
    let temp_path = format!("{}.tmp", path);
    let mut file = tokio::fs::File::create(&temp_path).await?;
    for (key, data, ttl) in current_data {
        match data {
            crate::storage::DataType::String(value) => {
                let cmd = if let Some(ttl_duration) = ttl {
                    RespValue::Array(vec![
                        RespValue::BulkString("SETEX".to_string()),
                        RespValue::BulkString(key),
                        RespValue::BulkString(ttl_duration.as_secs().to_string()),
                        RespValue::BulkString(value),
                    ])
                } else {
                    RespValue::Array(vec![
                        RespValue::BulkString("SET".to_string()),
                        RespValue::BulkString(key),
                        RespValue::BulkString(value),
                    ])
                };
                file.write_all(cmd.encode().as_bytes()).await?;
            }
            crate::storage::DataType::List(list) => {
                if !list.is_empty() {
                    let mut cmd_parts = vec![
                        RespValue::BulkString("RPUSH".to_string()),
                        RespValue::BulkString(key.clone()),
                    ];
                    for item in list {
                        cmd_parts.push(RespValue::BulkString(item));
                    }
                    let cmd = RespValue::Array(cmd_parts);
                    file.write_all(cmd.encode().as_bytes()).await?;
                }
                if let Some(ttl_duration) = ttl {
                    let expire_cmd = RespValue::Array(vec![
                        RespValue::BulkString("EXPIRE".to_string()),
                        RespValue::BulkString(key),
                        RespValue::BulkString(ttl_duration.as_secs().to_string()),
                    ]);
                    file.write_all(expire_cmd.encode().as_bytes()).await?;
                }
            }
            _ => {}
        }
    }
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temp_path, path).await?;
    Ok(())
}
