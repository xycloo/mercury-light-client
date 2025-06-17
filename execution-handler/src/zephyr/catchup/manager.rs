use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::zephyr::serverless::{execute_function, FInput};
use crate::make_continue;

use super::types::{CatchupFunction, CatchupStatus, StatusMessage, StatusRequest, ZephyrCatchupManager};

impl ZephyrCatchupManager {
    pub fn new(binary_path: String, function_receiver: mpsc::Receiver<CatchupFunction>, status_receiver: mpsc::Receiver<StatusMessage>) -> Self {
        Self { function_receiver, status_receiver, jobs: HashMap::new(), binary_path }
    }

    pub async fn consume(&mut self) {
        loop {
            tokio::select! {
                biased;

                message = self.status_receiver.recv() => {
                    if let Some(message) = message {
                        self.handle_status_request(message);
                    }
                }

                request = self.function_receiver.recv() => {
                    make_continue!(self.execute_relayed_function(request).await);
                }
            };
        }
    }

    fn handle_status_request(&self, message: StatusMessage) {
        match message {
            StatusMessage::CatchupStatusRequest(StatusRequest { hash, sender }) => {
                if let Some(job) = self.jobs.get(&hash) {
                    let _ = sender.send(CatchupStatus::new(job.current, job.last)).unwrap();
                } else {
                    let _ = sender.send(CatchupStatus::default()).unwrap();
                }
            }
        }
    }
    
    async fn execute_relayed_function(&mut self, request: Option<CatchupFunction>) -> anyhow::Result<()> {
        if let Some(CatchupFunction { hash, function, current }) = request {
            let _ = execute_function(function, &self.binary_path).await;
            let job = self.jobs.get_mut(&hash).ok_or(anyhow::anyhow!("catchup doesn't exist"))?;
            job.current = current;
        }

        Ok(())
    }
}

