use FerroDB::storage::*;
use std::thread;
use std::time::Duration;
#[test]
fn test_set_and_get() {
    let store = FerroStore::new();
    store.set("name".to_string(), "ferro".to_string());

    assert_eq!(store.get("name"), Some("ferro".to_string()));
    assert_eq!(store.get("nonexistent"), None);
}
#[test]
fn test_delete_existing_key() {
    let store = FerroStore::new();
    store.set("key1".to_string(), "value1".to_string());

    // Delete should return true (key existed)
    assert!(store.delete("key1"));

    // Key should be gone
    assert_eq!(store.get("key1"), None);
}

#[test]
fn test_delete_nonexistent_key() {
    let store = FerroStore::new();

    // Delete nonexistent key returns false
    assert!(!store.delete("nonexistent"));
}

#[test]
fn test_exists() {
    let store = FerroStore::new();
    store.set("key1".to_string(), "value1".to_string());

    assert!(store.exists("key1"));
    assert!(!store.exists("nonexistent"));
}
#[test]
fn test_set_with_expiry() {
    let store = FerroStore::new();

    // Set with 2 second expiry
    store.set_with_expiry("temp".to_string(), "data".to_string(), 2);

    // Should exist immediately
    assert_eq!(store.get("temp"), Some("data".to_string()));

    // Wait 3 seconds
    thread::sleep(Duration::from_secs(3));

    // Should be expired and return None
    assert_eq!(store.get("temp"), None);
}

#[test]
fn test_expire_command() {
    let store = FerroStore::new();

    // Set key without expiration
    store.set("key".to_string(), "value".to_string());

    // Add expiration
    assert!(store.expire("key", 2));

    // Should still exist
    assert_eq!(store.get("key"), Some("value".to_string()));

    // Wait for expiration
    thread::sleep(Duration::from_secs(3));

    // Should be gone
    assert_eq!(store.get("key"), None);
}

#[test]
fn test_expire_nonexistent_key() {
    let store = FerroStore::new();

    // Can't set expiration on nonexistent key
    assert!(!store.expire("nonexistent", 10));
}

#[test]
fn test_ttl_no_expiration() {
    let store = FerroStore::new();
    store.set("key".to_string(), "value".to_string());

    // Key with no expiration returns -1
    assert_eq!(store.ttl("key"), Some(-1));
}

#[test]
fn test_ttl_with_expiration() {
    let store = FerroStore::new();
    store.set_with_expiry("key".to_string(), "value".to_string(), 10);

    // TTL should be around 10 seconds (allow some margin)
    let ttl = store.ttl("key").unwrap();
    assert!((8..=10).contains(&ttl));
}

#[test]
fn test_ttl_nonexistent_key() {
    let store = FerroStore::new();

    // Nonexistent key returns None
    assert_eq!(store.ttl("nonexistent"), None);
}

#[test]
fn test_persist_command() {
    let store = FerroStore::new();

    // Set with expiration
    store.set_with_expiry("key".to_string(), "value".to_string(), 5);
    assert!(store.ttl("key").unwrap() > 0);

    // Remove expiration
    assert!(store.persist("key"));

    // Should now have no expiration
    assert_eq!(store.ttl("key"), Some(-1));
}

#[test]
fn test_persist_key_without_expiration() {
    let store = FerroStore::new();
    store.set("key".to_string(), "value".to_string());

    // Persisting a key without expiration returns false
    assert!(!store.persist("key"));
}

#[test]
fn test_exists_with_expired_key() {
    let store = FerroStore::new();
    store.set_with_expiry("key".to_string(), "value".to_string(), 1);

    assert!(store.exists("key"));

    thread::sleep(Duration::from_secs(2));

    assert!(!store.exists("key"));
}

#[test]
fn test_delete_expired_keys() {
    let store = FerroStore::new();

    // Set multiple keys with different expirations
    store.set_with_expiry("short".to_string(), "val1".to_string(), 1);
    store.set_with_expiry("medium".to_string(), "val2".to_string(), 10);
    store.set("permanent".to_string(), "val3".to_string());

    thread::sleep(Duration::from_secs(2));

    // Clean up expired keys
    let deleted = store.delete_expired_keys();

    // Should have deleted 1 key ("short")
    assert_eq!(deleted, 1);

    // Verify states
    assert_eq!(store.get("short"), None);
    assert_eq!(store.get("medium"), Some("val2".to_string()));
    assert_eq!(store.get("permanent"), Some("val3".to_string()));
}
#[test]
fn test_lpush_single_value() {
    let store = FerroStore::new();

    let len = store.lpush("mylist", vec!["hello".to_string()]).unwrap();
    assert_eq!(len, 1);
}

#[test]
fn test_lpush_multiple_values() {
    let store = FerroStore::new();

    // LPUSH mylist "world" "hello"
    let len = store
        .lpush("mylist", vec!["world".to_string(), "hello".to_string()])
        .unwrap();
    assert_eq!(len, 2);

    // Order should be: ["hello", "world"]
    let items = store.lrange("mylist", 0, -1).unwrap();
    assert_eq!(items, vec!["hello", "world"]);
}

#[test]
fn test_rpush_multiple_values() {
    let store = FerroStore::new();

    let len = store
        .rpush("mylist", vec!["hello".to_string(), "world".to_string()])
        .unwrap();
    assert_eq!(len, 2);

    // Order should be: ["hello", "world"]
    let items = store.lrange("mylist", 0, -1).unwrap();
    assert_eq!(items, vec!["hello", "world"]);
}

#[test]
fn test_lpush_on_string_key_fails() {
    let store = FerroStore::new();

    // Set a string value
    store.set("mykey".to_string(), "myvalue".to_string());

    // LPUSH on string key should fail
    let result = store.lpush("mykey", vec!["value".to_string()]);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("WRONGTYPE"));
}

#[test]
fn test_lpop_single() {
    let store = FerroStore::new();

    store
        .lpush(
            "mylist",
            vec!["c".to_string(), "b".to_string(), "a".to_string()],
        )
        .unwrap();
    // List is now: ["a", "b", "c"]

    let popped = store.lpop("mylist", None).unwrap();
    assert_eq!(popped, vec!["a"]);

    // List is now: ["b", "c"]
    let remaining = store.lrange("mylist", 0, -1).unwrap();
    assert_eq!(remaining, vec!["b", "c"]);
}

#[test]
fn test_lpop_multiple() {
    let store = FerroStore::new();

    store
        .lpush(
            "mylist",
            vec![
                "e".to_string(),
                "d".to_string(),
                "c".to_string(),
                "b".to_string(),
                "a".to_string(),
            ],
        )
        .unwrap();
    // List: ["a", "b", "c", "d", "e"]

    let popped = store.lpop("mylist", Some(3)).unwrap();
    assert_eq!(popped, vec!["a", "b", "c"]);

    let remaining = store.lrange("mylist", 0, -1).unwrap();
    assert_eq!(remaining, vec!["d", "e"]);
}

#[test]
fn test_rpop_single() {
    let store = FerroStore::new();

    store
        .rpush(
            "mylist",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .unwrap();

    let popped = store.rpop("mylist", None).unwrap();
    assert_eq!(popped, vec!["c"]);

    let remaining = store.lrange("mylist", 0, -1).unwrap();
    assert_eq!(remaining, vec!["a", "b"]);
}

#[test]
fn test_rpop_multiple() {
    let store = FerroStore::new();

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

    let popped = store.rpop("mylist", Some(2)).unwrap();
    assert_eq!(popped, vec!["e", "d"]);

    let remaining = store.lrange("mylist", 0, -1).unwrap();
    assert_eq!(remaining, vec!["a", "b", "c"]);
}

#[test]
fn test_lpop_empty_list() {
    let store = FerroStore::new();

    let popped = store.lpop("nonexistent", None).unwrap();
    assert_eq!(popped, Vec::<String>::new());
}

#[test]
fn test_list_gets_deleted_when_empty() {
    let store = FerroStore::new();

    store.lpush("mylist", vec!["only".to_string()]).unwrap();
    store.lpop("mylist", None).unwrap();

    // Key should not exist anymore
    assert!(!store.exists("mylist"));
}

#[test]
fn test_llen() {
    let store = FerroStore::new();

    assert_eq!(store.llen("mylist").unwrap(), 0);

    store
        .lpush(
            "mylist",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .unwrap();
    assert_eq!(store.llen("mylist").unwrap(), 3);
}

#[test]
fn test_llen_on_string_fails() {
    let store = FerroStore::new();
    store.set("mykey".to_string(), "value".to_string());

    let result = store.llen("mykey");
    assert!(result.is_err());
}

#[test]
fn test_lrange_basic() {
    let store = FerroStore::new();
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

    // Get all
    let all = store.lrange("mylist", 0, -1).unwrap();
    assert_eq!(all, vec!["a", "b", "c", "d", "e"]);

    // Get first 3
    let first_three = store.lrange("mylist", 0, 2).unwrap();
    assert_eq!(first_three, vec!["a", "b", "c"]);

    // Get last 2
    let last_two = store.lrange("mylist", -2, -1).unwrap();
    assert_eq!(last_two, vec!["d", "e"]);

    // Get middle
    let middle = store.lrange("mylist", 1, 3).unwrap();
    assert_eq!(middle, vec!["b", "c", "d"]);
}

#[test]
fn test_lrange_out_of_bounds() {
    let store = FerroStore::new();
    store
        .rpush("mylist", vec!["a".to_string(), "b".to_string()])
        .unwrap();

    // Start beyond end
    let empty = store.lrange("mylist", 10, 20).unwrap();
    assert_eq!(empty, Vec::<String>::new());

    // Stop beyond end (should return what's available)
    let available = store.lrange("mylist", 0, 100).unwrap();
    assert_eq!(available, vec!["a", "b"]);
}
#[test]
fn test_sadd_basic() {
    let store = FerroStore::new();

    let added = store
        .sadd("myset", vec!["apple".to_string(), "banana".to_string()])
        .unwrap();
    assert_eq!(added, 2);

    // Add duplicate
    let added = store.sadd("myset", vec!["apple".to_string()]).unwrap();
    assert_eq!(added, 0);
}

#[test]
fn test_smembers() {
    let store = FerroStore::new();

    store
        .sadd(
            "myset",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .unwrap();
    let members = store.smembers("myset").unwrap();

    assert_eq!(members.len(), 3);
    assert!(members.contains(&"a".to_string()));
    assert!(members.contains(&"b".to_string()));
    assert!(members.contains(&"c".to_string()));
}

#[test]
fn test_sismember() {
    let store = FerroStore::new();

    store.sadd("myset", vec!["apple".to_string()]).unwrap();

    assert_eq!(store.sismember("myset", "apple").unwrap(), true);
    assert_eq!(store.sismember("myset", "banana").unwrap(), false);
}

#[test]
fn test_srem() {
    let store = FerroStore::new();

    store
        .sadd(
            "myset",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .unwrap();

    let removed = store.srem("myset", vec!["b".to_string()]).unwrap();
    assert_eq!(removed, 1);

    let members = store.smembers("myset").unwrap();
    assert_eq!(members.len(), 2);
    assert!(!members.contains(&"b".to_string()));
}

#[test]
fn test_scard() {
    let store = FerroStore::new();

    store
        .sadd("myset", vec!["a".to_string(), "b".to_string()])
        .unwrap();
    assert_eq!(store.scard("myset").unwrap(), 2);
}

#[test]
fn test_sinter() {
    let store = FerroStore::new();

    store
        .sadd(
            "set1",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .unwrap();
    store
        .sadd(
            "set2",
            vec!["b".to_string(), "c".to_string(), "d".to_string()],
        )
        .unwrap();

    let inter = store
        .sinter(vec!["set1".to_string(), "set2".to_string()])
        .unwrap();
    assert_eq!(inter.len(), 2);
    assert!(inter.contains(&"b".to_string()));
    assert!(inter.contains(&"c".to_string()));
}

#[test]
fn test_sunion() {
    let store = FerroStore::new();

    store
        .sadd("set1", vec!["a".to_string(), "b".to_string()])
        .unwrap();
    store
        .sadd("set2", vec!["b".to_string(), "c".to_string()])
        .unwrap();

    let union = store
        .sunion(vec!["set1".to_string(), "set2".to_string()])
        .unwrap();
    assert_eq!(union.len(), 3);
    assert!(union.contains(&"a".to_string()));
    assert!(union.contains(&"b".to_string()));
    assert!(union.contains(&"c".to_string()));
}

#[test]
fn test_sdiff() {
    let store = FerroStore::new();

    store
        .sadd(
            "set1",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .unwrap();
    store
        .sadd("set2", vec!["b".to_string(), "d".to_string()])
        .unwrap();

    let diff = store
        .sdiff(vec!["set1".to_string(), "set2".to_string()])
        .unwrap();
    assert_eq!(diff.len(), 2);
    assert!(diff.contains(&"a".to_string()));
    assert!(diff.contains(&"c".to_string()));
}

// ============ SORTED SET TESTS ============

#[test]
fn test_zadd_basic() {
    let store = FerroStore::new();

    let added = store
        .zadd(
            "leaderboard",
            vec![(100.0, "alice".to_string()), (200.0, "bob".to_string())],
        )
        .unwrap();

    assert_eq!(added, 2);
}

#[test]
fn test_zadd_update_score() {
    let store = FerroStore::new();

    store
        .zadd("leaderboard", vec![(100.0, "alice".to_string())])
        .unwrap();
    let added = store
        .zadd("leaderboard", vec![(150.0, "alice".to_string())])
        .unwrap();

    // Should not count as new addition
    assert_eq!(added, 0);

    // Score should be updated
    assert_eq!(store.zscore("leaderboard", "alice").unwrap(), Some(150.0));
}

#[test]
fn test_zscore() {
    let store = FerroStore::new();

    store
        .zadd("leaderboard", vec![(100.0, "alice".to_string())])
        .unwrap();

    assert_eq!(store.zscore("leaderboard", "alice").unwrap(), Some(100.0));
    assert_eq!(store.zscore("leaderboard", "bob").unwrap(), None);
}

#[test]
fn test_zrange() {
    let store = FerroStore::new();

    store
        .zadd(
            "leaderboard",
            vec![
                (100.0, "alice".to_string()),
                (200.0, "bob".to_string()),
                (150.0, "charlie".to_string()),
            ],
        )
        .unwrap();

    let range = store.zrange("leaderboard", 0, -1, false).unwrap();
    assert_eq!(range, vec!["alice", "charlie", "bob"]);
}

#[test]
fn test_zrange_with_scores() {
    let store = FerroStore::new();

    store
        .zadd(
            "leaderboard",
            vec![(100.0, "alice".to_string()), (200.0, "bob".to_string())],
        )
        .unwrap();

    let range = store.zrange("leaderboard", 0, -1, true).unwrap();
    assert_eq!(range, vec!["alice", "100", "bob", "200"]);
}

#[test]
fn test_zrank() {
    let store = FerroStore::new();

    store
        .zadd(
            "leaderboard",
            vec![
                (100.0, "alice".to_string()),
                (200.0, "bob".to_string()),
                (150.0, "charlie".to_string()),
            ],
        )
        .unwrap();

    assert_eq!(store.zrank("leaderboard", "alice").unwrap(), Some(0));
    assert_eq!(store.zrank("leaderboard", "charlie").unwrap(), Some(1));
    assert_eq!(store.zrank("leaderboard", "bob").unwrap(), Some(2));
    assert_eq!(store.zrank("leaderboard", "nobody").unwrap(), None);
}

#[test]
fn test_zrem() {
    let store = FerroStore::new();

    store
        .zadd(
            "leaderboard",
            vec![(100.0, "alice".to_string()), (200.0, "bob".to_string())],
        )
        .unwrap();

    let removed = store
        .zrem("leaderboard", vec!["alice".to_string()])
        .unwrap();
    assert_eq!(removed, 1);

    assert_eq!(store.zcard("leaderboard").unwrap(), 1);
}

#[test]
fn test_zcard() {
    let store = FerroStore::new();

    store
        .zadd(
            "leaderboard",
            vec![(100.0, "alice".to_string()), (200.0, "bob".to_string())],
        )
        .unwrap();

    assert_eq!(store.zcard("leaderboard").unwrap(), 2);
}
