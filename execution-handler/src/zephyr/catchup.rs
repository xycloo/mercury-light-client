use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::zephyr::serverless::execute_function;

use super::serverless::FInput;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ScopedEventCatchup {
    contracts: Vec<String>,
    topic1s: Vec<String>,
    topic2s: Vec<String>,
    topic3s: Vec<String>,
    topic4s: Vec<String>,
    start: i64,
}

pub enum StatusMessage {}

pub struct ZephyrCatchupManager {
    function_receiver: mpsc::Receiver<((Vec<u8>, FInput), (u32, u32))>,
    status_receiver: mpsc::Receiver<StatusMessage>,
    // hashid, (current_step, total)
    jobs: HashMap<Vec<u8>, (u32, u32)>,
    binary_path: String,
}

impl ZephyrCatchupManager {
    pub fn new(binary_path: String, function_receiver: mpsc::Receiver<((Vec<u8>, FInput), (u32, u32))>, status_receiver: mpsc::Receiver<StatusMessage>) -> Self {
        Self { function_receiver, status_receiver, jobs: HashMap::new(), binary_path }
    }

    pub async fn consume(&mut self) {
        loop {
            tokio::select! {
                // prioritizing settlement requests vs overlay messages, doesn't change too much on the CE since settlement
                // still relies on overlay at some level but speeds up the process of getting the tx for the settler to
                // send to the base chain.
                biased;

                message = self.status_receiver.recv() => {
                    if let Some(message) = message {
                        // todo
                    }
                }

                request = self.function_receiver.recv() => {
                    self.execute_catchup(request).await;
                }
            };
        }
    }

    async fn execute_catchup(&mut self, request: Option<((Vec<u8>, FInput), (u32, u32))>) {
        if let Some(((hash, function), (current, last))) = request {
            let _ = execute_function(function, &self.binary_path).await;
            let job = self.jobs.entry(hash).or_insert(Default::default());
            job.0 = current;
            job.1 = last;
        }
    }
}
