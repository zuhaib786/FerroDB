use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct FerroStore {
    db: Arc<RwLock<HashMap<String, ValueWithExpiry>>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortedSetData {
    pub scores: BTreeMap<OrderedFloat<f64>, HashSet<String>>,
    pub members: HashMap<String, OrderedFloat<f64>>,
}

impl Default for SortedSetData {
    fn default() -> Self {
        SortedSetData::new()
    }
}

impl SortedSetData {
    pub fn new() -> Self {
        Self {
            scores: BTreeMap::new(),
            members: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

#[derive(Clone, Debug)]
pub enum DataType {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
    SortedSet(SortedSetData),
}

#[derive(Clone, Debug)]
struct ValueWithExpiry {
    data: DataType,
    expires_at: Option<Instant>,
}

impl ValueWithExpiry {
    fn new_string(value: String) -> Self {
        Self {
            data: DataType::String(value),
            expires_at: None,
        }
    }
    fn new_string_with_expiry(value: String, ttl: Duration) -> Self {
        Self {
            data: DataType::String(value),
            expires_at: Some(Instant::now() + ttl),
        }
    }

    fn new_list() -> Self {
        Self {
            data: DataType::List(VecDeque::new()),
            expires_at: None,
        }
    }

    fn new_sorted_set() -> Self {
        Self {
            data: DataType::SortedSet(SortedSetData::new()),
            expires_at: None,
        }
    }

    fn new_set() -> Self {
        Self {
            data: DataType::Set(HashSet::new()),
            expires_at: None,
        }
    }

    fn is_expired(&self) -> bool {
        match self.expires_at {
            None => false,
            Some(expiry) => expiry <= Instant::now(),
        }
    }
    // NOTE: -2 => Expired , -1 => No expiry , i => i seconds till expiry
    fn ttl_seconds(&self) -> Option<i64> {
        match self.expires_at {
            None => Some(-1),
            Some(expiry) => {
                let now = Instant::now();
                if now >= expiry {
                    Some(-2)
                } else {
                    let remaining = expiry.duration_since(now);
                    Some(remaining.as_secs() as i64)
                }
            }
        }
    }
}

impl Default for FerroStore {
    fn default() -> Self {
        Self::new()
    }
}

impl FerroStore {
    pub fn new() -> Self {
        Self {
            db: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: String, value: String) {
        let mut db = self.db.write().unwrap();
        db.insert(key, ValueWithExpiry::new_string(value));
    }

    pub fn set_with_expiry(&self, key: String, value: String, ttl_seconds: u64) {
        let mut db = self.db.write().unwrap();
        let ttl = Duration::from_secs(ttl_seconds);
        db.insert(key, ValueWithExpiry::new_string_with_expiry(value, ttl));
    }

    /// Get a value, returning None if expired or doesnt exist.
    /// This is passive exploration
    pub fn get(&self, key: &str) -> Option<String> {
        let mut db = self.db.write().unwrap();
        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                db.remove(key);
                return None;
            }
            return match &entry.data {
                DataType::String(s) => Some(s.clone()),
                _ => None,
            };
        };
        None
    }

    pub fn exists(&self, key: &str) -> bool {
        let mut db = self.db.write().unwrap();
        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                db.remove(key);
                return false;
            }
            return true;
        }
        false
    }

    pub fn delete(&self, key: &str) -> bool {
        let mut db = self.db.write().unwrap();
        db.remove(key).is_some()
    }

    pub fn expire(&self, key: &str, ttl_seconds: u64) -> bool {
        let mut db = self.db.write().unwrap();

        if let Some(entry) = db.get_mut(key) {
            if entry.is_expired() {
                db.remove(key);
                return false;
            }

            let ttl = Duration::from_secs(ttl_seconds);
            entry.expires_at = Some(Instant::now() + ttl);
            return true;
        }

        false
    }

    /// Get TTL of a key in seconds
    /// Returns: Some(seconds) if key exists, None if key doesn't exist
    /// Special values: -1 = no expiration, -2 = expired
    pub fn ttl(&self, key: &str) -> Option<i64> {
        let db = self.db.read().unwrap();

        if let Some(entry) = db.get(key) {
            return entry.ttl_seconds();
        }

        None // Key doesn't exist
    }

    /// Remove expiration from a key (PERSIST command)
    /// Returns true if expiration was removed
    pub fn persist(&self, key: &str) -> bool {
        let mut db = self.db.write().unwrap();

        if let Some(entry) = db.get_mut(key) {
            if entry.is_expired() {
                db.remove(key);
                return false;
            }

            if entry.expires_at.is_some() {
                entry.expires_at = None;
                return true;
            }
        }

        false
    }

    /// Active expiration: Remove all expired keys
    /// Returns count of keys deleted
    pub fn delete_expired_keys(&self) -> usize {
        let mut db = self.db.write().unwrap();
        let mut to_delete = Vec::new();

        // Collect expired keys
        for (key, entry) in db.iter() {
            if entry.is_expired() {
                to_delete.push(key.clone());
            }
        }

        let count = to_delete.len();

        // Delete them
        for key in to_delete {
            db.remove(&key);
        }

        count
    }

    // ====== LIST OPERATIONS =====
    /// Push the values to the left(head) of list
    /// Creates the list if it doesnt exist
    ///Returns new Length of the list
    pub fn lpush(&self, key: &str, values: Vec<String>) -> Result<usize, String> {
        let mut db = self.db.write().unwrap();

        let entry = db
            .entry(key.to_string())
            .or_insert(ValueWithExpiry::new_list());
        if entry.is_expired() {
            *entry = ValueWithExpiry::new_list();
        }

        match &mut entry.data {
            DataType::List(list) => {
                for value in values.into_iter() {
                    list.push_front(value);
                }
                Ok(list.len())
            }
            _ => {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
            }
        }
    }
    pub fn rpush(&self, key: &str, values: Vec<String>) -> Result<usize, String> {
        let mut db = self.db.write().unwrap();

        let entry = db
            .entry(key.to_string())
            .or_insert(ValueWithExpiry::new_list());
        if entry.is_expired() {
            *entry = ValueWithExpiry::new_list();
        }

        match &mut entry.data {
            DataType::List(list) => {
                for value in values.into_iter() {
                    list.push_back(value);
                }
                Ok(list.len())
            }
            _ => {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
            }
        }
    }
    pub fn lpop(&self, key: &str, count: Option<usize>) -> Result<Vec<String>, String> {
        let mut db = self.db.write().unwrap();

        if let Some(entry) = db.get_mut(key) {
            if entry.is_expired() {
                db.remove(key);
                return Ok(vec![]);
            }

            match &mut entry.data {
                DataType::List(list) => {
                    let count = count.unwrap_or(1);

                    let mut result: Vec<String> = Vec::new();
                    for _ in 0..count {
                        if let Some(value) = list.pop_front() {
                            result.push(value);
                        } else {
                            break;
                        }
                    }
                    if list.is_empty() {
                        db.remove(key);
                    }
                    Ok(result)
                }
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(vec![])
        }
    }
    pub fn rpop(&self, key: &str, count: Option<usize>) -> Result<Vec<String>, String> {
        let mut db = self.db.write().unwrap();

        if let Some(entry) = db.get_mut(key) {
            if entry.is_expired() {
                db.remove(key);
                return Ok(vec![]);
            }

            match &mut entry.data {
                DataType::List(list) => {
                    let count = count.unwrap_or(1);

                    let mut result: Vec<String> = Vec::new();
                    for _ in 0..count {
                        if let Some(value) = list.pop_back() {
                            result.push(value);
                        } else {
                            break;
                        }
                    }
                    if list.is_empty() {
                        db.remove(key);
                    }
                    Ok(result)
                }
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(vec![])
        }
    }

    pub fn llen(&self, key: &str) -> Result<usize, String> {
        let mut db = self.db.write().unwrap();

        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                db.remove(key);
                return Ok(0);
            }

            match &entry.data {
                DataType::List(list) => Ok(list.len()),
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(0)
        }
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, String> {
        let mut db = self.db.write().unwrap();
        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                db.remove(key);
                return Ok(vec![]);
            }
            match &entry.data {
                DataType::List(list) => {
                    let len = list.len() as i64;
                    let start = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    };
                    let stop = if stop < 0 {
                        (len + stop).max(-1)
                    } else {
                        stop.min(len - 1)
                    };
                    if start > stop || start >= len {
                        return Ok(vec![]);
                    }
                    let result = list
                        .iter()
                        .skip(start as usize)
                        .take((stop - start + 1) as usize)
                        .cloned()
                        .collect();
                    Ok(result)
                }

                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(vec![])
        }
    }

    // Set Functions
    pub fn sadd(&self, key: &str, members: Vec<String>) -> Result<usize, String> {
        let mut db = self.db.write().unwrap();
        let entry = db
            .entry(key.to_string())
            .or_insert(ValueWithExpiry::new_set());
        if entry.is_expired() {
            *entry = ValueWithExpiry::new_set();
        }

        match &mut entry.data {
            DataType::Set(set) => {
                let mut added = 0;
                for member in members {
                    if set.insert(member) {
                        added += 1;
                    }
                }
                Ok(added)
            }
            _ => {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
            }
        }
    }

    pub fn srem(&self, key: &str, members: Vec<String>) -> Result<usize, String> {
        let mut db = self.db.write().unwrap();
        if let Some(entry) = db.get_mut(key) {
            if entry.is_expired() {
                db.remove(key);
                return Ok(0);
            }

            match &mut entry.data {
                DataType::Set(set) => {
                    let mut removed = 0;
                    for member in members {
                        if set.remove(&member) {
                            removed += 1;
                        }
                    }
                    if set.is_empty() {
                        db.remove(key);
                    }
                    Ok(removed)
                }
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(0)
        }
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>, String> {
        let mut db = self.db.write().unwrap();

        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                db.remove(key);
                return Ok(vec![]);
            }
            match &entry.data {
                DataType::Set(set) => Ok(set.iter().cloned().collect()),
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(vec![])
        }
    }

    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, String> {
        let mut db = self.db.write().unwrap();

        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                db.remove(key);
                return Ok(false);
            }
            match &entry.data {
                DataType::Set(set) => Ok(set.contains(member)),
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(false)
        }
    }

    pub fn scard(&self, key: &str) -> Result<usize, String> {
        let mut db = self.db.write().unwrap();

        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                db.remove(key);
                return Ok(0);
            }
            match &entry.data {
                DataType::Set(set) => Ok(set.len()),
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(0)
        }
    }

    pub fn sinter(&self, keys: Vec<String>) -> Result<Vec<String>, String> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let db = self.db.read().unwrap();
        let first_key = &keys[0];
        let mut result: Option<HashSet<String>> = None;
        if let Some(entry) = db.get(first_key) {
            if !entry.is_expired() {
                if let DataType::Set(set) = &entry.data {
                    result = Some(set.clone());
                } else {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }
        }
        if result.is_none() {
            return Ok(vec![]);
        }

        let mut result_set = result.unwrap();
        for key in &keys[1..] {
            if let Some(entry) = db.get(key) {
                if !entry.is_expired() {
                    if let DataType::Set(set) = &entry.data {
                        result_set = result_set.intersection(set).cloned().collect();
                    } else {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        );
                    }
                }
            } else {
                // If any set doesn't exist, intersection is empty
                return Ok(vec![]);
            }
        }

        Ok(result_set.into_iter().collect())
    }
    pub fn sunion(&self, keys: Vec<String>) -> Result<Vec<String>, String> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let db = self.db.read().unwrap();
        let mut result_set = HashSet::new();

        for key in keys {
            if let Some(entry) = db.get(&key) {
                if !entry.is_expired() {
                    if let DataType::Set(set) = &entry.data {
                        result_set = result_set.union(set).cloned().collect();
                    } else {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        );
                    }
                }
            }
        }

        Ok(result_set.into_iter().collect())
    }
    pub fn sdiff(&self, keys: Vec<String>) -> Result<Vec<String>, String> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let db = self.db.read().unwrap();

        // Get first set
        let first_key = &keys[0];
        let mut result_set = HashSet::new();

        if let Some(entry) = db.get(first_key) {
            if !entry.is_expired() {
                if let DataType::Set(set) = &entry.data {
                    result_set = set.clone();
                } else {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }
        }

        // Subtract remaining sets
        for key in &keys[1..] {
            if let Some(entry) = db.get(key) {
                if !entry.is_expired() {
                    if let DataType::Set(set) = &entry.data {
                        result_set = result_set.difference(set).cloned().collect();
                    } else {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        );
                    }
                }
            }
        }

        Ok(result_set.into_iter().collect())
    }
    pub fn zadd(&self, key: &str, members: Vec<(f64, String)>) -> Result<usize, String> {
        let mut db = self.db.write().unwrap();

        let entry = db
            .entry(key.to_string())
            .or_insert_with(|| ValueWithExpiry {
                data: DataType::SortedSet(SortedSetData::new()),
                expires_at: None,
            });

        if entry.is_expired() {
            *entry = ValueWithExpiry {
                data: DataType::SortedSet(SortedSetData::new()),
                expires_at: None,
            };
        }

        match &mut entry.data {
            DataType::SortedSet(zset) => {
                let mut added = 0;

                for (score, member) in members {
                    let score_key = OrderedFloat(score);

                    // Check if member already exists
                    if let Some(old_score) = zset.members.get(&member) {
                        // Remove from old score bucket
                        if let Some(bucket) = zset.scores.get_mut(old_score) {
                            bucket.remove(&member);
                            if bucket.is_empty() {
                                zset.scores.remove(old_score);
                            }
                        }
                    } else {
                        added += 1;
                    }

                    // Add to new score bucket
                    zset.scores
                        .entry(score_key)
                        .or_insert_with(HashSet::new)
                        .insert(member.clone());
                    zset.members.insert(member, score_key);
                }

                Ok(added)
            }
            _ => {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
            }
        }
    }

    /// Remove members from sorted set
    pub fn zrem(&self, key: &str, members: Vec<String>) -> Result<usize, String> {
        let mut db = self.db.write().unwrap();

        if let Some(entry) = db.get_mut(key) {
            if entry.is_expired() {
                db.remove(key);
                return Ok(0);
            }

            match &mut entry.data {
                DataType::SortedSet(zset) => {
                    let mut removed = 0;

                    for member in members {
                        if let Some(score) = zset.members.remove(&member) {
                            removed += 1;

                            if let Some(bucket) = zset.scores.get_mut(&score) {
                                bucket.remove(&member);
                                if bucket.is_empty() {
                                    zset.scores.remove(&score);
                                }
                            }
                        }
                    }

                    // Remove key if empty
                    if zset.is_empty() {
                        db.remove(key);
                    }

                    Ok(removed)
                }
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(0)
        }
    }

    /// Get score of a member
    pub fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, String> {
        let db = self.db.read().unwrap();

        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                return Ok(None);
            }

            match &entry.data {
                DataType::SortedSet(zset) => Ok(zset.members.get(member).map(|s| s.0)),
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(None)
        }
    }

    /// Get range of members by index (sorted by score)
    /// start and stop can be negative (count from end)
    pub fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Result<Vec<String>, String> {
        let db = self.db.read().unwrap();

        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                return Ok(vec![]);
            }

            match &entry.data {
                DataType::SortedSet(zset) => {
                    // Flatten to vector: (member, score)
                    let mut all_members: Vec<(String, f64)> = Vec::new();
                    for (score, members) in &zset.scores {
                        for member in members {
                            all_members.push((member.clone(), score.0));
                        }
                    }

                    let len = all_members.len() as i64;

                    // Convert negative indices
                    let start = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    };
                    let stop = if stop < 0 {
                        (len + stop).max(-1)
                    } else {
                        stop.min(len - 1)
                    };

                    if start > stop || start >= len {
                        return Ok(vec![]);
                    }

                    let range: Vec<String> = all_members
                        .into_iter()
                        .skip(start as usize)
                        .take((stop - start + 1) as usize)
                        .flat_map(|(member, score)| {
                            if with_scores {
                                vec![member, score.to_string()]
                            } else {
                                vec![member]
                            }
                        })
                        .collect();

                    Ok(range)
                }
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(vec![])
        }
    }

    /// Get rank (index) of member (0-based)
    pub fn zrank(&self, key: &str, member: &str) -> Result<Option<usize>, String> {
        let db = self.db.read().unwrap();

        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                return Ok(None);
            }

            match &entry.data {
                DataType::SortedSet(zset) => {
                    // Check if member exists
                    if !zset.members.contains_key(member) {
                        return Ok(None);
                    }

                    // Count members before this one
                    let target_score = zset.members.get(member).unwrap();
                    let mut rank = 0;

                    for (score, members) in &zset.scores {
                        if score < target_score {
                            rank += members.len();
                        } else if score == target_score {
                            // Count members in same score bucket that come before alphabetically
                            for m in members {
                                if m.as_str() < member {
                                    rank += 1;
                                } else if m == member {
                                    return Ok(Some(rank));
                                }
                            }
                        }
                    }

                    Ok(Some(rank))
                }
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(None)
        }
    }

    /// Get cardinality (size) of sorted set
    pub fn zcard(&self, key: &str) -> Result<usize, String> {
        let db = self.db.read().unwrap();

        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                return Ok(0);
            }

            match &entry.data {
                DataType::SortedSet(zset) => Ok(zset.len()),
                _ => Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        } else {
            Ok(0)
        }
    }

    // Storange Functions
    /// Create a snapshot for the database for persistance
    /// Returns: HashMap<Key, (DataType, Option<Instant>)>
    pub fn snapshot(&self) -> HashMap<String, (DataType, Option<Instant>)> {
        let db = self.db.read().unwrap();
        db.iter()
            .map(|(k, v)| (k.clone(), (v.data.clone(), v.expires_at)))
            .collect()
    }
    /// Load single entry(used during restore)
    pub fn load_entry(&self, key: String, data: DataType, ttl: Option<Duration>) {
        let mut db = self.db.write().unwrap();
        let expires_at = ttl.map(|d| Instant::now() + d);
        db.insert(key, ValueWithExpiry { data, expires_at });
    }

    /// Get number of keys (for stats)
    pub fn dbsize(&self) -> usize {
        self.db.read().unwrap().len()
    }
    pub fn get_all_data(&self) -> Vec<(String, DataType, Option<Duration>)> {
        let db = self.db.read().unwrap();

        db.iter()
            .filter_map(|(key, entry)| {
                if entry.is_expired() {
                    None
                } else {
                    let ttl = entry.expires_at.map(|instant| {
                        let now = Instant::now();
                        if instant > now {
                            instant.duration_since(now)
                        } else {
                            Duration::from_secs(0)
                        }
                    });
                    Some((key.clone(), entry.data.clone(), ttl))
                }
            })
            .collect()
    }
}
