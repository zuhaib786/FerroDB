use crate::aof::AofWriter;
use crate::protocol::RespValue;
use crate::pubsub::{ClientSubscriptions, PubSubHub};
use crate::storage::FerroStore;

pub async fn handle_command(
    value: RespValue,
    store: &FerroStore,
    aof: Option<&AofWriter>,
    pubsub: Option<&PubSubHub>,
    client_subs: Option<&mut ClientSubscriptions>,
) -> RespValue {
    // 1. Ensure that we recieved an array (Redis commands are always arrays)
    let cmd_array = match value {
        RespValue::Array(a) => a,
        _ => return RespValue::SimpleString("ERR expected array".to_string()),
    };
    // 2. Extract the command name
    //
    let cmd_name = match &cmd_array[0] {
        RespValue::BulkString(s) => s.to_uppercase(),
        _ => return RespValue::BulkString("ERR command must be a bulk string".to_string()),
    };

    if let Some(subs) = client_subs.as_ref()
        && subs.is_subscribed()
    {
        // In subscribe mode, only allow certain commands
        match cmd_name.as_str() {
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PING" | "QUIT" => {
                // Allowed in subscribe mode
            }
            _ => {
                return RespValue::SimpleString(
                    "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context"
                        .to_string(),
                );
            }
        }
    }

    let should_log = matches!(
        cmd_name.as_str(),
        "SET"
            | "DEL"
            | "EXPIRE"
            | "PERSIST"
            | "SETEX"
            | "MSET"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "SADD"
            | "SREM"
            | "ZADD"
            | "ZREM"
    );
    if should_log && let Some(aof_writer) = aof {
        aof_writer.log_command(&RespValue::Array(cmd_array.clone()));
    }
    // 3. Dispatch the correct logic
    match cmd_name.as_str() {
        "SET" => handle_set(&cmd_array, store),
        "GET" => handle_get(&cmd_array, store),
        "PING" => handle_ping(&cmd_array),
        "EXISTS" => handle_exists(&cmd_array, store),
        "DEL" => handle_del(&cmd_array, store),
        "MGET" => handle_mget(&cmd_array, store),
        "MSET" => handle_mset(&cmd_array, store),
        "EXPIRE" => handle_expire(&cmd_array, store),
        "TTL" => handle_ttl(&cmd_array, store),
        "PERSIST" => handle_persist(&cmd_array, store),
        "SETEX" => handle_setex(&cmd_array, store),
        // List Commands
        "LPUSH" => handle_lpush(&cmd_array, store),
        "RPUSH" => handle_rpush(&cmd_array, store),
        "LPOP" => handle_lpop(&cmd_array, store),
        "RPOP" => handle_rpop(&cmd_array, store),
        "LLEN" => handle_llen(&cmd_array, store),
        "LRANGE" => handle_lrange(&cmd_array, store),
        // Save operations
        "SAVE" => handle_save(&cmd_array, store).await,
        "BGSAVE" => handle_bgsave(&cmd_array, store),
        "LASTSAVE" => handle_lastsave(&cmd_array, store),
        "DBSIZE" => handle_dbsize(&cmd_array, store),
        "BGREWRITEAOF" => handle_bgrewriteaof(&cmd_array, store),

        // Sorted Set Operations
        "ZADD" => handle_zadd(&cmd_array, store),
        "ZREM" => handle_zrem(&cmd_array, store),
        "ZSCORE" => handle_zscore(&cmd_array, store),
        "ZRANGE" => handle_zrange(&cmd_array, store),
        "ZRANK" => handle_zrank(&cmd_array, store),
        "ZCARD" => handle_zcard(&cmd_array, store),

        // Set commands
        "SADD" => handle_sadd(&cmd_array, store),
        "SREM" => handle_srem(&cmd_array, store),
        "SMEMBERS" => handle_smembers(&cmd_array, store),
        "SISMEMBER" => handle_sismember(&cmd_array, store),
        "SCARD" => handle_scard(&cmd_array, store),
        "SINTER" => handle_sinter(&cmd_array, store),
        "SUNION" => handle_sunion(&cmd_array, store),
        "SDIFF" => handle_sdiff(&cmd_array, store),

        "SUBSCRIBE" => handle_subscribe(&cmd_array, pubsub, client_subs),
        "UNSUBSCRIBE" => handle_unsubscribe(&cmd_array, client_subs),
        "PUBLISH" => handle_publish(&cmd_array, pubsub),

        _ => RespValue::SimpleString(format!("ERR unknown command {}", cmd_name)),
    }
}

fn handle_set(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 3 {
        return RespValue::SimpleString("ERR wrong number of arguments for 'set'".to_string());
    }
    if let (RespValue::BulkString(k), RespValue::BulkString(v)) = (&cmd_array[1], &cmd_array[2]) {
        store.set(k.clone(), v.clone());
        RespValue::SimpleString("OK".to_string())
    } else {
        RespValue::SimpleString("ERR arguments must be bulk strings".to_string())
    }
}

fn handle_get(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 2 {
        return RespValue::SimpleString("ERR wrong number of arguments for get".to_string());
    }
    if let RespValue::BulkString(k) = &cmd_array[1] {
        match store.get(k) {
            Some(v) => RespValue::BulkString(v),
            None => RespValue::Null,
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_ping(cmd_array: &[RespValue]) -> RespValue {
    if cmd_array.len() == 1 {
        // PING with no args returns PONG
        RespValue::SimpleString("PONG".to_string())
    } else if cmd_array.len() == 2 {
        if let RespValue::BulkString(msg) = &cmd_array[1] {
            RespValue::BulkString(msg.clone())
        } else {
            RespValue::SimpleString("ERR wrong argument type".to_string())
        }
    } else {
        RespValue::SimpleString("ERR wrong number of arguments for 'ping'".to_string())
    }
}

fn handle_exists(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'exists' command".to_string(),
        );
    }
    let mut exists_count = 0;

    for key_value in &cmd_array[1..] {
        if let RespValue::BulkString(key) = key_value {
            if store.exists(key) {
                exists_count += 1;
            }
        } else {
            return RespValue::SimpleString("ERR all keys must be bulk strings".to_string());
        }
    }
    RespValue::Integer(exists_count)
}

fn handle_del(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    // DEL requires at least one key
    if cmd_array.len() < 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'del' command".to_string(),
        );
    }

    let mut deleted_count = 0;

    // Loop through all keys (starting from index 1, since 0 is "DEL")
    for key_value in &cmd_array[1..] {
        if let RespValue::BulkString(key) = key_value {
            // Delete returns true if key existed
            if store.delete(key) {
                deleted_count += 1;
            }
        } else {
            return RespValue::SimpleString("ERR all keys must be bulk strings".to_string());
        }
    }

    RespValue::Integer(deleted_count)
}

fn handle_mget(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'mget' command".to_string(),
        );
    }
    let mut res: Vec<RespValue> = vec![];
    for key_value in &cmd_array[1..] {
        if let RespValue::BulkString(s) = key_value {
            res.push(match store.get(s) {
                Some(value) => RespValue::BulkString(value),
                None => RespValue::Null,
            })
        } else {
            return RespValue::SimpleString("ERR all keys must be bulk strings".to_string());
        }
    }
    RespValue::Array(res)
}

fn handle_mset(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 2 {
        return RespValue::SimpleString("ERR Wrong number of arguments for 'mset'".to_string());
    }
    if cmd_array.len() % 2 != 1 {
        return RespValue::SimpleString("ERR Wrong number of arguments for 'mset'".to_string());
    }
    for key_value in &cmd_array[1..] {
        if let RespValue::BulkString(_) = key_value {
            continue;
        } else {
            return RespValue::SimpleString(
                "ERR all arguments to mset must be bulk strings".to_string(),
            );
        }
    }
    for i in (1..cmd_array.len()).step_by(2) {
        let key = &cmd_array[i];
        let value = &cmd_array[i + 1];
        if let RespValue::BulkString(k) = key
            && let RespValue::BulkString(v) = value
        {
            store.set(k.clone(), v.clone());
        }
    }
    RespValue::SimpleString("OK".to_string())
}

fn handle_expire(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 3 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'expire' command".to_string(),
        );
    }

    if let (RespValue::BulkString(key), RespValue::BulkString(seconds_str)) =
        (&cmd_array[1], &cmd_array[2])
    {
        // Parse seconds
        match seconds_str.parse::<u64>() {
            Ok(seconds) => {
                let result = store.expire(key, seconds);
                RespValue::Integer(if result { 1 } else { 0 })
            }
            Err(_) => {
                RespValue::SimpleString("ERR value is not an integer or out of range".to_string())
            }
        }
    } else {
        RespValue::SimpleString("ERR arguments must be bulk strings".to_string())
    }
}

fn handle_ttl(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'ttl' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        match store.ttl(key) {
            Some(ttl) => RespValue::Integer(ttl),
            None => RespValue::Integer(-2), // Key doesn't exist
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_persist(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'persist' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        let result = store.persist(key);
        RespValue::Integer(if result { 1 } else { 0 })
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_setex(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    // SETEX key seconds value
    if cmd_array.len() != 4 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'setex' command".to_string(),
        );
    }

    if let (
        RespValue::BulkString(key),
        RespValue::BulkString(seconds_str),
        RespValue::BulkString(value),
    ) = (&cmd_array[1], &cmd_array[2], &cmd_array[3])
    {
        match seconds_str.parse::<u64>() {
            Ok(seconds) => {
                store.set_with_expiry(key.clone(), value.clone(), seconds);
                RespValue::SimpleString("OK".to_string())
            }
            Err(_) => {
                RespValue::SimpleString("ERR value is not an integer or out of range".to_string())
            }
        }
    } else {
        RespValue::SimpleString("ERR arguments must be bulk strings".to_string())
    }
}

fn handle_lpush(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 3 {
        return RespValue::SimpleString(
            "ERR Wrong number of arguments for 'lpush' command".to_string(),
        );
    }
    if let RespValue::BulkString(key) = &cmd_array[1] {
        let mut values = Vec::new();
        for val in &cmd_array[2..] {
            if let RespValue::BulkString(s) = val {
                values.push(s.clone());
            } else {
                return RespValue::SimpleString("ERR all values must be bulk strings".to_string());
            }
        }
        match store.lpush(key, values) {
            Ok(len) => RespValue::Integer(len as i64),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_rpush(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 3 {
        return RespValue::SimpleString(
            "ERR Wrong number of arguments for 'lpush' command".to_string(),
        );
    }
    if let RespValue::BulkString(key) = &cmd_array[1] {
        let mut values = Vec::new();
        for val in &cmd_array[2..] {
            if let RespValue::BulkString(s) = val {
                values.push(s.clone());
            } else {
                return RespValue::SimpleString("ERR all values must be bulk strings".to_string());
            }
        }
        match store.rpush(key, values) {
            Ok(len) => RespValue::Integer(len as i64),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}
fn handle_lpop(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 2 || cmd_array.len() > 3 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'lpop' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        let count = if cmd_array.len() == 3 {
            if let RespValue::BulkString(count_str) = &cmd_array[2] {
                match count_str.parse::<usize>() {
                    Ok(c) => Some(c),
                    Err(_) => {
                        return RespValue::SimpleString("ERR value is not an integer".to_string());
                    }
                }
            } else {
                return RespValue::SimpleString("ERR count must be a bulk string".to_string());
            }
        } else {
            None
        };

        match store.lpop(key, count) {
            Ok(values) => {
                if values.is_empty() {
                    RespValue::Null
                } else if count.is_none() {
                    // Single pop returns single value
                    RespValue::BulkString(values[0].clone())
                } else {
                    // Multiple pop returns array
                    RespValue::Array(values.into_iter().map(RespValue::BulkString).collect())
                }
            }
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_rpop(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 2 || cmd_array.len() > 3 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'rpop' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        let count = if cmd_array.len() == 3 {
            if let RespValue::BulkString(count_str) = &cmd_array[2] {
                match count_str.parse::<usize>() {
                    Ok(c) => Some(c),
                    Err(_) => {
                        return RespValue::SimpleString("ERR value is not an integer".to_string());
                    }
                }
            } else {
                return RespValue::SimpleString("ERR count must be a bulk string".to_string());
            }
        } else {
            None
        };

        match store.rpop(key, count) {
            Ok(values) => {
                if values.is_empty() {
                    RespValue::Null
                } else if count.is_none() {
                    RespValue::BulkString(values[0].clone())
                } else {
                    RespValue::Array(values.into_iter().map(RespValue::BulkString).collect())
                }
            }
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_llen(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'llen' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        match store.llen(key) {
            Ok(len) => RespValue::Integer(len as i64),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_lrange(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 4 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'lrange' command".to_string(),
        );
    }

    if let (
        RespValue::BulkString(key),
        RespValue::BulkString(start_str),
        RespValue::BulkString(stop_str),
    ) = (&cmd_array[1], &cmd_array[2], &cmd_array[3])
    {
        let start = match start_str.parse::<i64>() {
            Ok(s) => s,
            Err(_) => return RespValue::SimpleString("ERR value is not an integer".to_string()),
        };

        let stop = match stop_str.parse::<i64>() {
            Ok(s) => s,
            Err(_) => return RespValue::SimpleString("ERR value is not an integer".to_string()),
        };

        match store.lrange(key, start, stop) {
            Ok(values) => RespValue::Array(values.into_iter().map(RespValue::BulkString).collect()),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR arguments must be bulk strings".to_string())
    }
}

async fn handle_save(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 1 {
        return RespValue::SimpleString(
            "ERR Wrong number of arguments for 'save' command".to_string(),
        );
    }

    match crate::persistance::save_rdb(store, "dump.rdb").await {
        Ok(_) => RespValue::SimpleString("OK".to_string()),
        Err(e) => RespValue::SimpleString(format!("ERR {}", e)),
    }
}

fn handle_bgsave(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 1 {
        return RespValue::SimpleString(
            "ERR Wrong number of arguments for 'save' command".to_string(),
        );
    }
    let store_clone = store.clone();
    tokio::spawn(async move {
        match crate::persistance::save_rdb(&store_clone, "dump.rdb").await {
            Ok(_) => println!("Background save completed"),
            Err(e) => println!("Background save failed : {}", e),
        }
    });
    RespValue::SimpleString("Background saving started".to_string())
}
fn handle_lastsave(_cmd_array: &[RespValue], _store: &FerroStore) -> RespValue {
    // TODO: Track last save timestamp
    RespValue::Integer(0)
}

fn handle_dbsize(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 1 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'dbsize' command".to_string(),
        );
    }

    RespValue::Integer(store.dbsize() as i64)
}
fn handle_bgrewriteaof(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 1 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'bgrewriteaof' command".to_string(),
        );
    }

    let data = store.get_all_data();

    tokio::spawn(async move {
        match crate::aof::rewrite_aof(data, "appendonly.aof").await {
            Ok(_) => println!("AOF rewrite completed"),
            Err(e) => eprintln!("AOF rewrite failed: {}", e),
        }
    });

    RespValue::SimpleString("Background AOF rewrite started".to_string())
}

fn handle_sadd(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 3 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'sadd' command".to_string(),
        );
    }
    if let RespValue::BulkString(key) = &cmd_array[1] {
        let mut members = Vec::new();

        for val in &cmd_array[2..] {
            if let RespValue::BulkString(v) = val {
                members.push(v.clone());
            } else {
                return RespValue::SimpleString("ERR all members must be bulk strings".to_string());
            }
        }
        match store.sadd(key, members) {
            Ok(added) => RespValue::Integer(added as i64),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}
fn handle_srem(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 3 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'srem' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        let mut members = Vec::new();

        for val in &cmd_array[2..] {
            if let RespValue::BulkString(v) = val {
                members.push(v.clone());
            } else {
                return RespValue::SimpleString("ERR all members must be bulk strings".to_string());
            }
        }

        match store.srem(key, members) {
            Ok(removed) => RespValue::Integer(removed as i64),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_smembers(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'smembers' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        match store.smembers(key) {
            Ok(members) => {
                RespValue::Array(members.into_iter().map(RespValue::BulkString).collect())
            }
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_sismember(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 3 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'sismember' command".to_string(),
        );
    }

    if let (RespValue::BulkString(key), RespValue::BulkString(member)) =
        (&cmd_array[1], &cmd_array[2])
    {
        match store.sismember(key, member) {
            Ok(exists) => RespValue::Integer(if exists { 1 } else { 0 }),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR arguments must be bulk strings".to_string())
    }
}

fn handle_scard(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'scard' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        match store.scard(key) {
            Ok(size) => RespValue::Integer(size as i64),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_sinter(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'sinter' command".to_string(),
        );
    }

    let mut keys = Vec::new();
    for val in &cmd_array[1..] {
        if let RespValue::BulkString(k) = val {
            keys.push(k.clone());
        } else {
            return RespValue::SimpleString("ERR all keys must be bulk strings".to_string());
        }
    }

    match store.sinter(keys) {
        Ok(members) => RespValue::Array(members.into_iter().map(RespValue::BulkString).collect()),
        Err(e) => RespValue::SimpleString(format!("-{}", e)),
    }
}

fn handle_sunion(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'sunion' command".to_string(),
        );
    }

    let mut keys = Vec::new();
    for val in &cmd_array[1..] {
        if let RespValue::BulkString(k) = val {
            keys.push(k.clone());
        } else {
            return RespValue::SimpleString("ERR all keys must be bulk strings".to_string());
        }
    }

    match store.sunion(keys) {
        Ok(members) => RespValue::Array(members.into_iter().map(RespValue::BulkString).collect()),
        Err(e) => RespValue::SimpleString(format!("-{}", e)),
    }
}

fn handle_sdiff(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'sdiff' command".to_string(),
        );
    }

    let mut keys = Vec::new();
    for val in &cmd_array[1..] {
        if let RespValue::BulkString(k) = val {
            keys.push(k.clone());
        } else {
            return RespValue::SimpleString("ERR all keys must be bulk strings".to_string());
        }
    }

    match store.sdiff(keys) {
        Ok(members) => RespValue::Array(members.into_iter().map(RespValue::BulkString).collect()),
        Err(e) => RespValue::SimpleString(format!("-{}", e)),
    }
}

// ============ SORTED SET COMMAND HANDLERS ============

fn handle_zadd(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    // ZADD key score member [score member ...]
    if cmd_array.len() < 4 || !(cmd_array.len() - 2).is_multiple_of(2) {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'zadd' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        let mut members = Vec::new();

        // Parse score-member pairs
        let mut i = 2;
        while i < cmd_array.len() {
            if let (RespValue::BulkString(score_str), RespValue::BulkString(member)) =
                (&cmd_array[i], &cmd_array[i + 1])
            {
                match score_str.parse::<f64>() {
                    Ok(score) => members.push((score, member.clone())),
                    Err(_) => {
                        return RespValue::SimpleString(
                            "ERR value is not a valid float".to_string(),
                        );
                    }
                }
            } else {
                return RespValue::SimpleString("ERR syntax error".to_string());
            }
            i += 2;
        }

        match store.zadd(key, members) {
            Ok(added) => RespValue::Integer(added as i64),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_zrem(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() < 3 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'zrem' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        let mut members = Vec::new();

        for val in &cmd_array[2..] {
            if let RespValue::BulkString(v) = val {
                members.push(v.clone());
            } else {
                return RespValue::SimpleString("ERR all members must be bulk strings".to_string());
            }
        }

        match store.zrem(key, members) {
            Ok(removed) => RespValue::Integer(removed as i64),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}

fn handle_zscore(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 3 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'zscore' command".to_string(),
        );
    }

    if let (RespValue::BulkString(key), RespValue::BulkString(member)) =
        (&cmd_array[1], &cmd_array[2])
    {
        match store.zscore(key, member) {
            Ok(Some(score)) => RespValue::BulkString(score.to_string()),
            Ok(None) => RespValue::Null,
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR arguments must be bulk strings".to_string())
    }
}

fn handle_zrange(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    // ZRANGE key start stop [WITHSCORES]
    if cmd_array.len() < 4 || cmd_array.len() > 5 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'zrange' command".to_string(),
        );
    }

    if let (
        RespValue::BulkString(key),
        RespValue::BulkString(start_str),
        RespValue::BulkString(stop_str),
    ) = (&cmd_array[1], &cmd_array[2], &cmd_array[3])
    {
        let start = match start_str.parse::<i64>() {
            Ok(s) => s,
            Err(_) => return RespValue::SimpleString("ERR value is not an integer".to_string()),
        };

        let stop = match stop_str.parse::<i64>() {
            Ok(s) => s,
            Err(_) => return RespValue::SimpleString("ERR value is not an integer".to_string()),
        };

        // Check for WITHSCORES flag
        let with_scores = if cmd_array.len() == 5 {
            if let RespValue::BulkString(flag) = &cmd_array[4] {
                flag.to_uppercase() == "WITHSCORES"
            } else {
                return RespValue::SimpleString("ERR syntax error".to_string());
            }
        } else {
            false
        };

        match store.zrange(key, start, stop, with_scores) {
            Ok(values) => RespValue::Array(values.into_iter().map(RespValue::BulkString).collect()),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR arguments must be bulk strings".to_string())
    }
}

fn handle_zrank(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 3 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'zrank' command".to_string(),
        );
    }

    if let (RespValue::BulkString(key), RespValue::BulkString(member)) =
        (&cmd_array[1], &cmd_array[2])
    {
        match store.zrank(key, member) {
            Ok(Some(rank)) => RespValue::Integer(rank as i64),
            Ok(None) => RespValue::Null,
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR arguments must be bulk strings".to_string())
    }
}

fn handle_zcard(cmd_array: &[RespValue], store: &FerroStore) -> RespValue {
    if cmd_array.len() != 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'zcard' command".to_string(),
        );
    }

    if let RespValue::BulkString(key) = &cmd_array[1] {
        match store.zcard(key) {
            Ok(size) => RespValue::Integer(size as i64),
            Err(e) => RespValue::SimpleString(format!("-{}", e)),
        }
    } else {
        RespValue::SimpleString("ERR key must be a bulk string".to_string())
    }
}
fn handle_subscribe(
    cmd_array: &[RespValue],
    pubsub: Option<&PubSubHub>,
    client_subs: Option<&mut ClientSubscriptions>,
) -> RespValue {
    if cmd_array.len() < 2 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'subscribe' command".to_string(),
        );
    }

    let Some(hub) = pubsub else {
        return RespValue::SimpleString("ERR pub/sub not available".to_string());
    };

    let Some(subs) = client_subs else {
        return RespValue::SimpleString("ERR subscription tracking not available".to_string());
    };

    let mut responses = Vec::new();

    for channel_val in &cmd_array[1..] {
        if let RespValue::BulkString(channel) = channel_val {
            // Subscribe to channel
            let receiver = hub.subscribe(channel);
            subs.add(channel.clone(), receiver);

            // Return subscription confirmation
            // Format: ["subscribe", channel, subscription_count]
            responses.push(RespValue::Array(vec![
                RespValue::BulkString("subscribe".to_string()),
                RespValue::BulkString(channel.clone()),
                RespValue::Integer(subs.count() as i64),
            ]));
        } else {
            return RespValue::SimpleString("ERR channel names must be bulk strings".to_string());
        }
    }

    // Return array of all subscription confirmations
    if responses.len() == 1 {
        responses.into_iter().next().unwrap()
    } else {
        RespValue::Array(responses)
    }
}

fn handle_unsubscribe(
    cmd_array: &[RespValue],
    client_subs: Option<&mut ClientSubscriptions>,
) -> RespValue {
    let Some(subs) = client_subs else {
        return RespValue::SimpleString("ERR subscription tracking not available".to_string());
    };

    if cmd_array.len() == 1 {
        // UNSUBSCRIBE with no args = unsubscribe from all
        let channels: Vec<String> = subs.channels();
        let mut responses = Vec::new();

        for channel in channels {
            subs.remove(&channel);
            responses.push(RespValue::Array(vec![
                RespValue::BulkString("unsubscribe".to_string()),
                RespValue::BulkString(channel),
                RespValue::Integer(subs.count() as i64),
            ]));
        }

        if responses.is_empty() {
            // Not subscribed to anything
            return RespValue::Array(vec![
                RespValue::BulkString("unsubscribe".to_string()),
                RespValue::Null,
                RespValue::Integer(0),
            ]);
        }

        if responses.len() == 1 {
            responses.into_iter().next().unwrap()
        } else {
            RespValue::Array(responses)
        }
    } else {
        // UNSUBSCRIBE specific channels
        let mut responses = Vec::new();

        for channel_val in &cmd_array[1..] {
            if let RespValue::BulkString(channel) = channel_val {
                subs.remove(channel);
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString("unsubscribe".to_string()),
                    RespValue::BulkString(channel.clone()),
                    RespValue::Integer(subs.count() as i64),
                ]));
            } else {
                return RespValue::SimpleString(
                    "ERR channel names must be bulk strings".to_string(),
                );
            }
        }

        if responses.len() == 1 {
            responses.into_iter().next().unwrap()
        } else {
            RespValue::Array(responses)
        }
    }
}

fn handle_publish(cmd_array: &[RespValue], pubsub: Option<&PubSubHub>) -> RespValue {
    if cmd_array.len() != 3 {
        return RespValue::SimpleString(
            "ERR wrong number of arguments for 'publish' command".to_string(),
        );
    }

    let Some(hub) = pubsub else {
        return RespValue::SimpleString("ERR pub/sub not available".to_string());
    };

    if let (RespValue::BulkString(channel), RespValue::BulkString(message)) =
        (&cmd_array[1], &cmd_array[2])
    {
        let count = hub.publish(channel, message.clone());
        RespValue::Integer(count as i64)
    } else {
        RespValue::SimpleString("ERR arguments must be bulk strings".to_string())
    }
}
