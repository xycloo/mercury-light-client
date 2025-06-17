use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::zephyr::serverless::FInput;

pub struct CatchupFunction {
    pub hash: Vec<u8>,
    pub function: FInput,
    pub current: u32
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ScopedEventCatchup {
    contracts: Vec<String>,
    topic1s: Vec<String>,
    topic2s: Vec<String>,
    topic3s: Vec<String>,
    topic4s: Vec<String>,
    start: i64,
}

pub enum StatusMessage {
    CatchupStatusRequest(StatusRequest)
}

#[derive(Default, Clone, Debug)]
pub struct CatchupStatus {
    pub(super) current: u32,
    pub(super) last: u32
}

impl CatchupStatus {
    pub fn new(current: u32, last: u32) -> Self {
        Self { current, last }
    }
}

pub struct StatusRequest {
    pub(super) hash: Vec<u8>,
    pub(super) sender: oneshot::Sender<CatchupStatus>
}

pub enum CatchupRequest {
    Execute(ExecuteCatchupRequest),
}

pub struct CreateCatchupRequest {
    pub hash: Vec<u8>,
    pub total: u32
}

pub struct ExecuteCatchupRequest {
    pub hash: Vec<u8>,
    pub scoped: ScopedEventCatchup
}

pub struct ZephyrCatchupManager {
    pub(super) function_receiver: mpsc::Receiver<CatchupFunction>,
    pub status_receiver: mpsc::Receiver<StatusMessage>,
    // hashid, (current_step, total)
    pub jobs: HashMap<Vec<u8>, CatchupStatus>,
    pub binary_path: String,
}