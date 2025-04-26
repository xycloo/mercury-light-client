use base64::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::process::Command;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InvokeZephyrFunction {
    pub fname: String,
    arguments: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum BinaryType {
    Path(String),
    Code(Vec<u8>)
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FInput {
    pub associated_data: Vec<u8>,
    pub binary: BinaryType,
    pub user_id: u64,
    pub network_id: [u8; 32],
    pub fname: String
}

impl Config {
    pub fn function_from_invocation(&self, invocation: InvokeZephyrFunction, binary: Vec<u8>) -> FInput {
        FInput { associated_data: bincode::serialize(&invocation.arguments).unwrap(), binary:BinaryType::Code(binary), user_id:0, network_id: self.network_id(), fname: invocation.fname }
    }

    pub fn network_id(&self) -> [u8;32] {
        let mut hasher = Sha256::new();
        hasher.update(&self.network);
        hasher.finalize().as_slice().try_into().unwrap()
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub network: String,
    pub database_conn: String,
    pub executor_binary_path: String
}

pub async fn execute_function(function: FInput, executor_binary_path: &str) -> anyhow::Result<String> {
    let serialized = bincode::serialize(&function).unwrap();
    let encoded = BASE64_STANDARD.encode(&serialized);
    let child = Command::new(executor_binary_path)
        .arg(encoded.clone())
        .spawn().unwrap();

    let output = child.wait_with_output().await?;
    let resp = if output.status.success() {
        println!("[+] successfully called zephyr function");
        String::from_utf8(output.stdout).unwrap()
    } else {
        println!("Zephyr function error: {:?}", output);
        "invocation failed".to_string()
    };

    Ok(resp)
}