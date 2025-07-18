use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

use super::types::{
    CatchupFunction, CatchupStatus, StatusMessage, StatusRequest, ZephyrCatchupManager,
};
use crate::database::{ActorStatus, DbInstruction, DbInstructionWithCallback};
use crate::make_continue;
use crate::zephyr::serverless::{execute_function, BinaryType, FInput};
use tracing::info;

impl ZephyrCatchupManager {
    pub fn new(
        binary_path: String,
        function_receiver: mpsc::Receiver<CatchupFunction>,
        status_receiver: mpsc::Receiver<StatusMessage>,
        db_sender: mpsc::Sender<DbInstructionWithCallback>,
    ) -> Self {
        Self {
            function_receiver,
            status_receiver,
            jobs: HashMap::new(),
            binary_path,
            cached_binaries: HashMap::new(),
            db_sender,
        }
    }

    pub async fn consume(&mut self) {
        loop {
            tokio::select! {
                biased;

                message = self.status_receiver.recv() => {
                    info!("got new status request");
                    if let Some(message) = message {
                        self.handle_status_request(message);
                    }
                }

                request = self.function_receiver.recv() => {
                    info!("got new function to execute");
                    make_continue!(self.execute_relayed_function(request).await);
                }
            };
        }
    }

    pub(crate) async fn read_binary(&self, hash: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .db_sender
            .send(DbInstructionWithCallback::new(
                DbInstruction::RequestBinary(hash),
                tx,
            ))
            .await;

        match rx.await {
            Ok(ActorStatus::GotBinary(binary)) => Ok(binary),
            _ => Err(anyhow::anyhow!("failed to retrieve binary").into()),
        }
    }

    fn handle_status_request(&self, message: StatusMessage) {
        match message {
            StatusMessage::CatchupStatusRequest(StatusRequest { hash, sender }) => {
                if let Some(job) = self.jobs.get(&hash) {
                    let _ = sender
                        .send(CatchupStatus::new(job.current, job.last))
                        .unwrap();
                } else {
                    let _ = sender.send(CatchupStatus::default()).unwrap();
                }
            }
        }
    }

    async fn execute_relayed_function(
        &mut self,
        request: Option<CatchupFunction>,
    ) -> anyhow::Result<()> {
        if let Some(CatchupFunction {
            hash,
            function,
            current,
        }) = request
        {
            let binary = if let Some(raw) = self.cached_binaries.get(&hash) {
                raw.clone()
            } else {
                let code_raw = self.read_binary(hash.clone()).await?;
                let binary = BinaryType::Code(code_raw);
                self.cached_binaries.insert(hash.clone(), binary.clone());
                binary
            };

            let finput = FInput::from_stripped(function, &binary);
            let _ = execute_function(finput, &self.binary_path).await;
            let job: &mut CatchupStatus = self
                .jobs
                .get_mut(&hash)
                .ok_or(anyhow::anyhow!("catchup doesn't exist"))?;
            job.current = current;
        }

        Ok(())
    }
}
