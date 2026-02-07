use FerroDB::commands::*;
use FerroDB::protocol::*;
use FerroDB::storage::*;
#[tokio::test]
async fn test_set_get_flow() {
    let store = FerroStore::new();

    // 1. Simulate: SET "greet" "hello"
    let set_input = "*3\r\n$3\r\nSET\r\n$5\r\ngreet\r\n$5\r\nhello\r\n";
    let parsed_set = parse_resp(set_input).unwrap();
    let response_set = handle_command(parsed_set, &store, None).await;
    assert_eq!(response_set, RespValue::SimpleString("OK".to_string()));

    // 2. Simulate: GET "greet"
    let get_input = "*2\r\n$3\r\nGET\r\n$5\r\ngreet\r\n";
    let parsed_get = parse_resp(get_input).unwrap();
    let response_get = handle_command(parsed_get, &store, None).await;
    assert_eq!(response_get, RespValue::BulkString("hello".to_string()));
}
#[tokio::test]
async fn test_case_insensitive_commands() {
    let store = FerroStore::new();

    // SET in lowercase
    let set_input = "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let parsed = parse_resp(set_input).unwrap();
    let response = handle_command(parsed, &store, None).await;
    assert_eq!(response, RespValue::SimpleString("OK".to_string()));

    // GET in mixed case
    let get_input = "*2\r\n$3\r\nGeT\r\n$3\r\nkey\r\n";
    let parsed = parse_resp(get_input).unwrap();
    let response = handle_command(parsed, &store, None).await;
    assert_eq!(response, RespValue::BulkString("value".to_string()));
}
#[tokio::test]
async fn test_del_command() {
    let store = FerroStore::new();

    // Set a key
    store.set("key1".to_string(), "value1".to_string());

    // DEL returns number of keys removed
    let input = "*2\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;
    assert_eq!(response, RespValue::Integer(1));

    // Key should be gone
    assert_eq!(store.get("key1"), None);
}
#[tokio::test]
async fn test_del_single_key() {
    let store = FerroStore::new();

    // Set a key first
    store.set("mykey".to_string(), "myvalue".to_string());

    // DEL mykey
    let input = "*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    // Should return integer 1 (one key deleted)
    assert_eq!(response, RespValue::Integer(1));

    // Key should be gone
    assert_eq!(store.get("mykey"), None);
}
#[tokio::test]
async fn test_del_nonexistent_key() {
    let store = FerroStore::new();

    // DEL nonexistent
    let input = "*2\r\n$3\r\nDEL\r\n$11\r\nnonexistent\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    // Should return integer 0 (no keys deleted)
    assert_eq!(response, RespValue::Integer(0));
}

#[tokio::test]
async fn test_del_multiple_keys() {
    let store = FerroStore::new();

    // Set multiple keys
    store.set("key1".to_string(), "val1".to_string());
    store.set("key2".to_string(), "val2".to_string());

    // DEL key1 key2 key3 (key3 doesn't exist)
    let input = "*4\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    // Should return 2 (two keys deleted)
    assert_eq!(response, RespValue::Integer(2));
}

#[tokio::test]
async fn test_exists_single_key() {
    let store = FerroStore::new();
    store.set("mykey".to_string(), "myvalue".to_string());

    // EXISTS mykey
    let input = "*2\r\n$6\r\nEXISTS\r\n$5\r\nmykey\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    assert_eq!(response, RespValue::Integer(1));
}

#[tokio::test]
async fn test_exists_nonexistent_key() {
    let store = FerroStore::new();

    // EXISTS nonexistent
    let input = "*2\r\n$6\r\nEXISTS\r\n$11\r\nnonexistent\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    assert_eq!(response, RespValue::Integer(0));
}

#[tokio::test]
async fn test_exists_multiple_keys() {
    let store = FerroStore::new();
    store.set("key1".to_string(), "val1".to_string());
    store.set("key2".to_string(), "val2".to_string());

    // EXISTS key1 key2 key3 (key3 doesn't exist)
    let input = "*4\r\n$6\r\nEXISTS\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    // Should return 2 (two keys exist)
    assert_eq!(response, RespValue::Integer(2));
}
#[tokio::test]
async fn test_mget_multiple_keys() {
    let store = FerroStore::new();

    // Set some keys
    store.set("key1".to_string(), "value1".to_string());
    store.set("key2".to_string(), "value2".to_string());
    // key3 doesn't exist

    // MGET key1 key2 key3
    let input = "*4\r\n$4\r\nMGET\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    // Should return array with: ["value1", "value2", null]
    assert_eq!(
        response,
        RespValue::Array(vec![
            RespValue::BulkString("value1".to_string()),
            RespValue::BulkString("value2".to_string()),
            RespValue::Null,
        ])
    );
}

#[tokio::test]
async fn test_mget_all_nonexistent() {
    let store = FerroStore::new();

    // MGET key1 key2
    let input = "*3\r\n$4\r\nMGET\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    // Should return array of nulls
    assert_eq!(
        response,
        RespValue::Array(vec![RespValue::Null, RespValue::Null])
    );
}

#[tokio::test]
async fn test_mget_no_arguments() {
    let store = FerroStore::new();

    // MGET with no keys
    let input = "*1\r\n$4\r\nMGET\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    // Should return error
    match response {
        RespValue::SimpleString(msg) => assert!(msg.contains("ERR")),
        _ => panic!("Expected error message"),
    }
}

#[tokio::test]
async fn test_mset_multiple_pairs() {
    let store = FerroStore::new();

    // MSET key1 value1 key2 value2
    let input = "*5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    assert_eq!(response, RespValue::SimpleString("OK".to_string()));

    // Verify keys were set
    assert_eq!(store.get("key1"), Some("value1".to_string()));
    assert_eq!(store.get("key2"), Some("value2".to_string()));
}

#[tokio::test]
async fn test_mset_overwrites_existing() {
    let store = FerroStore::new();

    store.set("key1".to_string(), "old_value".to_string());

    // MSET key1 new_value
    let input = "*3\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$9\r\nnew_value\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    assert_eq!(response, RespValue::SimpleString("OK".to_string()));
    assert_eq!(store.get("key1"), Some("new_value".to_string()));
}

#[tokio::test]
async fn test_mset_odd_arguments() {
    let store = FerroStore::new();

    // MSET key1 value1 key2 (missing value for key2)
    let input = "*4\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    // Should return error
    match response {
        RespValue::SimpleString(msg) => {
            assert!(msg.contains("ERR") || msg.contains("Incorrect"))
        }
        _ => panic!("Expected error message"),
    }
}

#[tokio::test]
async fn test_mset_no_arguments() {
    let store = FerroStore::new();

    // MSET with no pairs
    let input = "*1\r\n$4\r\nMSET\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    match response {
        RespValue::SimpleString(msg) => assert!(msg.contains("Wrong") || msg.contains("ERR")),
        _ => panic!("Expected error message"),
    }
}
#[tokio::test]
async fn test_lpush_lpop_flow() {
    let store = FerroStore::new();

    // LPUSH mylist "world" "hello"
    let input = "*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$5\r\nworld\r\n$5\r\nhello\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;
    assert_eq!(response, RespValue::Integer(2));

    // LPOP mylist
    let input = "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;
    assert_eq!(response, RespValue::BulkString("hello".to_string()));
}

#[tokio::test]
async fn test_rpush_rpop_flow() {
    let store = FerroStore::new();

    // RPUSH mylist "a" "b" "c"
    let input = "*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;
    assert_eq!(response, RespValue::Integer(3));

    // RPOP mylist 2
    let input = "*3\r\n$4\r\nRPOP\r\n$6\r\nmylist\r\n$1\r\n2\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;
    assert_eq!(
        response,
        RespValue::Array(vec![
            RespValue::BulkString("c".to_string()),
            RespValue::BulkString("b".to_string()),
        ])
    );
}

#[tokio::test]
async fn test_lrange_command() {
    let store = FerroStore::new();

    // RPUSH mylist "a" "b" "c" "d" "e"
    store
        .rpush(
            "mylist",
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
            ],
        )
        .unwrap();

    // LRANGE mylist 0 2
    let input = "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$1\r\n2\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;
    assert_eq!(
        response,
        RespValue::Array(vec![
            RespValue::BulkString("a".to_string()),
            RespValue::BulkString("b".to_string()),
            RespValue::BulkString("c".to_string()),
        ])
    );
}

#[tokio::test]
async fn test_llen_command() {
    let store = FerroStore::new();

    // LPUSH mylist "a" "b" "c"
    store
        .lpush(
            "mylist",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .unwrap();

    // LLEN mylist
    let input = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;
    assert_eq!(response, RespValue::Integer(3));
}

#[tokio::test]
async fn test_lpush_on_string_key() {
    let store = FerroStore::new();

    // SET mykey "value"
    store.set("mykey".to_string(), "value".to_string());

    // LPUSH mykey "item" - should fail
    let input = "*3\r\n$5\r\nLPUSH\r\n$5\r\nmykey\r\n$4\r\nitem\r\n";
    let parsed = parse_resp(input).unwrap();
    let response = handle_command(parsed, &store, None).await;

    if let RespValue::SimpleString(msg) = response {
        assert!(msg.contains("WRONGTYPE"));
    } else {
        panic!("Expected error message");
    }
}
