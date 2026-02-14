use crate::storage::{DataType, FerroStore, SortedSetData};
use ordered_float::OrderedFloat;
use std::collections::{HashSet, VecDeque};
use std::io;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAGIC: &[u8] = b"FERRODB\0";
const VERSION: u8 = 1;

/// Serialize the database to RDB format
pub async fn save_rdb(store: &FerroStore, path: &str) -> io::Result<()> {
    let snapshot = store.snapshot();

    // Write to temp file first
    let temp_path = format!("{}.tmp", path);
    let mut file = File::create(&temp_path).await?;

    // Write header
    file.write_all(MAGIC).await?;
    file.write_u8(VERSION).await?;

    // Write number of keys
    file.write_u64(snapshot.len() as u64).await?;

    // Write each key-value pair
    for (key, (data, expiry)) in snapshot {
        // Write key
        write_string(&mut file, &key).await?;

        // Write data type and value
        match data {
            DataType::String(s) => {
                file.write_u8(0).await?; // Type: String
                write_string(&mut file, &s).await?;
            }
            DataType::List(list) => {
                file.write_u8(1).await?; // Type: List
                file.write_u64(list.len() as u64).await?;
                for item in list {
                    write_string(&mut file, &item).await?;
                }
            }
            DataType::Set(set) => {
                file.write_u8(2).await?; // Type: Set
                file.write_u64_le(set.len() as u64).await?;
                for member in set {
                    write_string(&mut file, &member).await?;
                }
            }
            DataType::SortedSet(zset) => {
                file.write_u8(3).await?; // Type: SortedSet
                file.write_u64_le(zset.len() as u64).await?;
                for (member, score) in &zset.members {
                    write_string(&mut file, member).await?;
                    file.write_f64_le(score.0).await?;
                }
            }
            _ => {}
        }

        // Write expiry
        match expiry {
            Some(instant) => {
                file.write_u8(1).await?; // Has expiry
                let now = Instant::now();
                let remaining = if instant > now {
                    instant.duration_since(now).as_secs() as i64
                } else {
                    0 // Already expired
                };
                file.write_i64(remaining).await?;
            }
            None => {
                file.write_u8(0).await?; // No expiry
            }
        }
    }

    file.sync_all().await?;
    drop(file);

    // Atomic rename
    tokio::fs::rename(&temp_path, path).await?;

    Ok(())
}

/// Deserialize RDB file and load into database
pub async fn load_rdb(store: &FerroStore, path: &str) -> io::Result<()> {
    let mut file = File::open(path).await?;

    // Read and verify header
    let mut magic = vec![0u8; 8];
    file.read_exact(&mut magic).await?;
    if magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid RDB file",
        ));
    }

    let version = file.read_u8().await?;
    if version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unsupported version: {}", version),
        ));
    }

    // Read number of keys
    let num_keys = file.read_u64().await?;

    // Read each key-value pair
    for _ in 0..num_keys {
        let key = read_string(&mut file).await?;

        let data_type = file.read_u8().await?;
        let data = match data_type {
            0 => {
                // String
                let value = read_string(&mut file).await?;
                DataType::String(value)
            }
            1 => {
                // List
                let list_len = file.read_u64().await?;
                let mut list = VecDeque::new();
                for _ in 0..list_len {
                    let item = read_string(&mut file).await?;
                    list.push_back(item);
                }
                DataType::List(list)
            }
            2 => {
                // Set
                let set_len = file.read_u64_le().await?;
                let mut set = HashSet::new();
                for _ in 0..set_len {
                    let member = read_string(&mut file).await?;
                    set.insert(member);
                }
                DataType::Set(set)
            }
            3 => {
                let zset_len = file.read_u64_le().await?;
                let mut zset = SortedSetData::new();
                for _ in 0..zset_len {
                    let member = read_string(&mut file).await?;
                    let score = file.read_f64_le().await?;

                    let score_key = OrderedFloat(score);
                    zset.scores
                        .entry(score_key)
                        .or_default()
                        .insert(member.clone());
                    zset.members.insert(member, score_key);
                }
                DataType::SortedSet(zset)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown data type: {}", data_type),
                ));
            }
        };

        let has_expiry = file.read_u8().await?;
        let expiry = if has_expiry == 1 {
            let remaining_secs = file.read_i64().await?;
            if remaining_secs > 0 {
                Some(Duration::from_secs(remaining_secs as u64))
            } else {
                None // Already expired
            }
        } else {
            None
        };

        // Load into store
        store.load_entry(key, data, expiry);
    }

    Ok(())
}

/// Helper: Write a string with length prefix
async fn write_string(file: &mut File, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    file.write_u64(bytes.len() as u64).await?;
    file.write_all(bytes).await?;
    Ok(())
}

/// Helper: Read a length-prefixed string
async fn read_string(file: &mut File) -> io::Result<String> {
    let len = file.read_u64().await?;
    let mut bytes = vec![0u8; len as usize];
    file.read_exact(&mut bytes).await?;
    String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
