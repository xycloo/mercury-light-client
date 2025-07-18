use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::{database::DbInstructionWithCallback, zephyr::serverless::BinaryType};

pub struct CatchupFunction {
    pub hash: Vec<u8>,
    pub function: StrippedFInput,
    pub current: u32,
}

impl CatchupFunction {
    pub fn new(hash: Vec<u8>, function: StrippedFInput, current: u32) -> Self {
        Self {
            hash,
            function,
            current,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ScopedEventCatchup {
    pub contracts: Vec<String>,
    pub topic1s: Vec<String>,
    pub topic2s: Vec<String>,
    pub topic3s: Vec<String>,
    pub topic4s: Vec<String>,
    pub start: i64,
}

pub enum StatusMessage {
    CatchupStatusRequest(StatusRequest),
}

#[derive(Default, Clone, Debug)]
pub struct CatchupStatus {
    pub(super) current: u32,
    pub(super) last: u32,
}

impl CatchupStatus {
    pub fn new(current: u32, last: u32) -> Self {
        Self { current, last }
    }
}

pub struct StatusRequest {
    pub(super) hash: Vec<u8>,
    pub(super) sender: oneshot::Sender<CatchupStatus>,
}

pub struct CreateCatchupRequest {
    pub hash: Vec<u8>,
    pub total: u32,
}

pub struct ExecuteCatchupRequest {
    pub hash: Vec<u8>,
    pub scoped: ScopedEventCatchup,
}

pub struct ZephyrCatchupManager {
    pub(super) function_receiver: mpsc::Receiver<CatchupFunction>,
    pub status_receiver: mpsc::Receiver<StatusMessage>,
    // hashid, (current_step, total)
    pub jobs: HashMap<Vec<u8>, CatchupStatus>,
    pub binary_path: String,
    pub cached_binaries: HashMap<Vec<u8>, BinaryType>,
    pub db_sender: mpsc::Sender<DbInstructionWithCallback>,
}

#[derive(Deserialize, Serialize, Debug)]
/// FInput but without actual binary because we infer it from local cache for efficiency.
pub struct StrippedFInput {
    pub associated_data: Vec<u8>,
    pub user_id: u64,
    pub network_id: [u8; 32],
    pub fname: String,
}

impl StrippedFInput {
    pub fn new(associated: Vec<u8>, fname: String, network_id: [u8; 32]) -> Self {
        Self {
            associated_data: associated,
            user_id: 0,
            network_id,
            fname,
        }
    }
}
