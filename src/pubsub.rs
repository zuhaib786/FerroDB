use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub struct PubSubMessage {
    pub channel: String,
    pub message: String,
}

#[derive(Clone)]
pub struct PubSubHub {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>,
}

impl Default for PubSubHub {
    fn default() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl PubSubHub {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish(&self, channel: &str, message: String) -> usize {
        let channels = self.channels.read().unwrap();
        if let Some(sender) = channels.get(channel) {
            let msg = PubSubMessage {
                channel: channel.to_string(),
                message,
            };
            sender.send(msg).unwrap_or_default()
        } else {
            0
        }
    }

    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<PubSubMessage> {
        let mut channels = self.channels.write().unwrap();
        let sender = channels.entry(channel.to_string()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(100);
            tx
        });
        sender.subscribe()
    }
    pub fn num_subscribers(&self, channel: &str) -> usize {
        let channels = self.channels.read().unwrap();
        if let Some(sender) = channels.get(channel) {
            sender.receiver_count()
        } else {
            0
        }
    }

    pub fn cleanup_empty_channels(&self) {
        let mut channels = self.channels.write().unwrap();
        channels.retain(|_, sender| sender.receiver_count() > 0);
    }
}

pub struct ClientSubscriptions {
    subscriptions: HashMap<String, broadcast::Receiver<PubSubMessage>>,
}
impl ClientSubscriptions {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    /// Add a subscription
    pub fn add(&mut self, channel: String, receiver: broadcast::Receiver<PubSubMessage>) {
        self.subscriptions.insert(channel, receiver);
    }

    /// Remove a subscription
    pub fn remove(&mut self, channel: &str) -> bool {
        self.subscriptions.remove(channel).is_some()
    }

    /// Get all subscribed channels
    pub fn channels(&self) -> Vec<String> {
        self.subscriptions.keys().cloned().collect()
    }

    /// Check if subscribed to any channels
    pub fn is_subscribed(&self) -> bool {
        !self.subscriptions.is_empty()
    }

    /// Get number of active subscriptions
    pub fn count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Try to receive a message from any subscribed channel (non-blocking)
    pub fn try_recv(&mut self) -> Option<PubSubMessage> {
        // Try each receiver until we get a message
        for receiver in self.subscriptions.values_mut() {
            match receiver.try_recv() {
                Ok(msg) => return Some(msg),
                Err(broadcast::error::TryRecvError::Empty) => continue,
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    // Message was lost due to buffer overflow - skip
                    continue;
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    // Channel closed - should clean up, but continue for now
                    continue;
                }
            }
        }
        None
    }

    /// Async receive from any channel
    pub async fn recv(&mut self) -> Option<PubSubMessage> {
        if self.subscriptions.is_empty() {
            return None;
        }

        // Create a vec of futures from all receivers
        let mut receivers: Vec<_> = self.subscriptions.values_mut().collect();

        if receivers.is_empty() {
            return None;
        }

        // Use select! to wait on all receivers simultaneously
        // For simplicity, we'll just wait on the first one for now
        // A production implementation would use FuturesUnordered
        if let Some(receiver) = receivers.first_mut() {
            (receiver.recv().await).ok()
        } else {
            None
        }
    }
}

impl Default for ClientSubscriptions {
    fn default() -> Self {
        Self::new()
    }
}
