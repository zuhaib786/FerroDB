use FerroDB::persistance::{load_rdb, save_rdb};
use FerroDB::storage::FerroStore;
use std::fs;
use tokio;

#[tokio::test]
async fn test_save_and_load_strings() {
    let store = FerroStore::new();

    // Add some data
    store.set("key1".to_string(), "value1".to_string());
    store.set("key2".to_string(), "value2".to_string());
    store.set("key3".to_string(), "value3".to_string());

    // Save to disk
    let path = "/tmp/test_FerroDB.rdb";
    save_rdb(&store, path).await.unwrap();

    // Create new store and load
    let new_store = FerroStore::new();
    load_rdb(&new_store, path).await.unwrap();

    // Verify data
    assert_eq!(new_store.get("key1"), Some("value1".to_string()));
    assert_eq!(new_store.get("key2"), Some("value2".to_string()));
    assert_eq!(new_store.get("key3"), Some("value3".to_string()));
    assert_eq!(new_store.get("nonexistent"), None);

    // Cleanup
    fs::remove_file(path).ok();
}

#[tokio::test]
async fn test_save_and_load_lists() {
    let store = FerroStore::new();

    // Add lists
    store
        .lpush(
            "list1",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .unwrap();
    store
        .rpush(
            "list2",
            vec!["x".to_string(), "y".to_string(), "z".to_string()],
        )
        .unwrap();

    // Save and load
    let path = "/tmp/test_FerroDB_lists.rdb";
    save_rdb(&store, path).await.unwrap();

    let new_store = FerroStore::new();
    load_rdb(&new_store, path).await.unwrap();

    // Verify lists
    let list1 = new_store.lrange("list1", 0, -1).unwrap();
    assert_eq!(list1, vec!["c", "b", "a"]);

    let list2 = new_store.lrange("list2", 0, -1).unwrap();
    assert_eq!(list2, vec!["x", "y", "z"]);

    fs::remove_file(path).ok();
}

#[tokio::test]
async fn test_save_and_load_with_expiry() {
    let store = FerroStore::new();

    // Add keys with and without expiry
    store.set("permanent".to_string(), "value".to_string());
    store.set_with_expiry("temporary".to_string(), "value".to_string(), 10);

    // Save and load
    let path = "/tmp/test_FerroDB_expiry.rdb";
    save_rdb(&store, path).await.unwrap();

    let new_store = FerroStore::new();
    load_rdb(&new_store, path).await.unwrap();

    // Verify
    assert_eq!(new_store.get("permanent"), Some("value".to_string()));
    assert_eq!(new_store.get("temporary"), Some("value".to_string()));

    // Check TTL
    assert_eq!(new_store.ttl("permanent"), Some(-1)); // No expiry
    let ttl = new_store.ttl("temporary").unwrap();
    assert!(ttl > 0 && ttl <= 10); // Has expiry

    fs::remove_file(path).ok();
}

#[tokio::test]
async fn test_save_empty_database() {
    let store = FerroStore::new();

    let path = "/tmp/test_FerroDB_empty.rdb";
    save_rdb(&store, path).await.unwrap();

    let new_store = FerroStore::new();
    load_rdb(&new_store, path).await.unwrap();

    assert_eq!(new_store.dbsize(), 0);

    fs::remove_file(path).ok();
}

#[tokio::test]
async fn test_load_nonexistent_file() {
    let store = FerroStore::new();

    let result = load_rdb(&store, "/tmp/nonexistent_FerroDB.rdb").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_mixed_data_types() {
    let store = FerroStore::new();

    // Mix of everything
    store.set("string1".to_string(), "value1".to_string());
    store
        .lpush("list1", vec!["a".to_string(), "b".to_string()])
        .unwrap();
    store.set_with_expiry("expiring".to_string(), "temp".to_string(), 30);
    store.rpush("list2", vec!["x".to_string()]).unwrap();

    let path = "/tmp/test_FerroDB_mixed.rdb";
    save_rdb(&store, path).await.unwrap();

    let new_store = FerroStore::new();
    load_rdb(&new_store, path).await.unwrap();

    // Verify all types
    assert_eq!(new_store.get("string1"), Some("value1".to_string()));
    assert_eq!(new_store.lrange("list1", 0, -1).unwrap(), vec!["b", "a"]);
    assert_eq!(new_store.get("expiring"), Some("temp".to_string()));
    assert_eq!(new_store.lrange("list2", 0, -1).unwrap(), vec!["x"]);
    assert_eq!(new_store.dbsize(), 4);

    fs::remove_file(path).ok();
}
