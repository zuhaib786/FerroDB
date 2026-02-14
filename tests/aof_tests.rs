use FerroDB::aof::{AofWriter, load_aof, rewrite_aof};
use FerroDB::commands::handle_command;
use FerroDB::protocol::parse_resp;
use FerroDB::storage::{DataType, FerroStore};
use std::collections::VecDeque;
use std::fs;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_aof_logging_and_replay() {
    let path = "/tmp/test_aof.log";
    fs::remove_file(path).ok();

    // Create AOF writer
    let (aof_writer, aof_handle) = AofWriter::new(path.to_string());

    // Spawn AOF background task
    tokio::spawn(async move {
        aof_handle.run().await.ok();
    });

    let store = FerroStore::new();

    // Execute some commands
    let cmd1 = parse_resp("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n").unwrap();
    handle_command(cmd1, &store, Some(&aof_writer), None, None).await;

    let cmd2 = parse_resp("*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n").unwrap();
    handle_command(cmd2, &store, Some(&aof_writer), None, None).await;

    // Wait for AOF to flush
    sleep(Duration::from_secs(2)).await;

    // Create new store and replay
    let new_store = FerroStore::new();
    let store_clone = new_store.clone();

    let count = load_aof(path, move |cmd| {
        let s = store_clone.clone();
        tokio::spawn(async move {
            handle_command(cmd, &s, None, None, None).await;
        });
    })
    .await
    .unwrap();

    sleep(Duration::from_millis(100)).await; // Wait for async replays

    assert_eq!(count, 2);
    assert_eq!(new_store.get("key1"), Some("value1".to_string()));
    assert_eq!(new_store.get("key2"), Some("value2".to_string()));

    fs::remove_file(path).ok();
}

#[tokio::test]
async fn test_aof_rewrite() {
    let path = "/tmp/test_aof_rewrite.log";
    fs::remove_file(path).ok();

    // Create data to rewrite
    let mut list = VecDeque::new();
    list.push_back("item1".to_string());
    list.push_back("item2".to_string());

    let data = vec![
        (
            "key1".to_string(),
            DataType::String("value1".to_string()),
            None,
        ),
        (
            "key2".to_string(),
            DataType::String("value2".to_string()),
            Some(Duration::from_secs(100)),
        ),
        ("mylist".to_string(), DataType::List(list), None),
    ];

    rewrite_aof(data, path).await.unwrap();

    // Replay and verify
    let store = FerroStore::new();
    let store_clone = store.clone();

    let command_count = load_aof(path, move |cmd| {
        let s = store_clone.clone();
        tokio::spawn(async move {
            handle_command(cmd, &s, None, None, None).await;
        });
    })
    .await
    .unwrap();

    assert_eq!(command_count, 3);
    sleep(Duration::from_millis(100)).await;

    assert_eq!(store.get("key1"), Some("value1".to_string()));
    assert_eq!(store.get("key2"), Some("value2".to_string()));
    assert_eq!(
        store.lrange("mylist", 0, -1).unwrap(),
        vec!["item1", "item2"]
    );

    fs::remove_file(path).ok();
}
