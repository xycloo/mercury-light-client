use serde::{Deserialize, Serialize};
use sha2::Digest;
use tokio::{sync::{mpsc, oneshot}, task::JoinHandle};
use tokio_postgres::{Client, NoTls};
use wasmi::StackLimits;

use crate::CodeUploadClient;

const MIN_VALUE_STACK_HEIGHT: usize = 1024;
const MAX_VALUE_STACK_HEIGHT: usize = 2 * 1024 * MIN_VALUE_STACK_HEIGHT;
const MAX_RECURSION_DEPTH: usize = 1024;

#[derive(Serialize, Deserialize)]
pub enum ActorStatus {
    Uploaded,
    NotWhitelisted,
    FailedToGetBinary,
    GotBinary(Vec<u8>)
}

#[derive(Debug, Clone)]
pub struct ConstructedZephyrBinary {
    pub code: Vec<u8>,    
    pub is_contract: bool,
    pub contracts: Option<Vec<String>>,
}

pub struct DbInstructionWithCallback {
    instruction: DbInstruction,
    callback_send: oneshot::Sender<ActorStatus>
}

impl DbInstructionWithCallback {
    pub fn new(instruction: DbInstruction, oneshot: oneshot::Sender<ActorStatus>) -> Self {
        Self { instruction, callback_send: oneshot }
    }
}

pub enum DbInstruction {
    UploadBinary(CodeUploadClient),

    RequestBinary(Vec<u8>),
    
    /// Fetches all whitelisted from the database. TODO this can greatly improved for perf on sync.
    SyncWhitelisted
}

pub struct PgConnectionActor {
    whitelisted: Vec<Vec<u8>>,
    receiver: mpsc::Receiver<DbInstructionWithCallback>,
    client: Client,
    _connection_task: JoinHandle<()>,
}

impl PgConnectionActor {
    pub async fn new(rx: mpsc::Receiver<DbInstructionWithCallback>,conn: &str) -> Result<Self, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::connect(conn, NoTls).await?;

        let connection_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {}", e);
            }
        });

        Ok(Self {
            receiver: rx,
            client,
            _connection_task: connection_task,
            whitelisted: Default::default()
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    async fn upload(
        &self,
        hash: &Vec<u8>,
        code: &Vec<u8>,
        is_contract: bool,
        contracts: Vec<String>,
    ) -> anyhow::Result<()> {
        // nb: validates the binary before allowing the upload.
        {
            let mut config = wasmi::Config::default();    
            config.compilation_mode(wasmi::CompilationMode::Lazy);

            let stack_limits = StackLimits::new(
                MIN_VALUE_STACK_HEIGHT,
                MAX_VALUE_STACK_HEIGHT,
                MAX_RECURSION_DEPTH,
            ).map_err(|_| anyhow::anyhow!("invalid stack limits"))?; // todo here
    
            // TODO: decide which post-mvp features to override.
            // For now we use wasmtime's defaults.
            config.consume_fuel(true);
            config.set_stack_limits(stack_limits);
    
            let engine = wasmi::Engine::new(&config);
            wasmi::Module::validate(&engine, &code)?;
        }
    
        let stmt = self.client
            .prepare_typed(
                "INSERT INTO public.zephyr_programs (code, is_contract, contracts, hash) VALUES ($1, $2, $3, $4)",
                &[
                    tokio_postgres::types::Type::BYTEA,
                    tokio_postgres::types::Type::BOOL,
                    tokio_postgres::types::Type::TEXT_ARRAY,
                    tokio_postgres::types::Type::BYTEA,
                ],
            )
            .await
            ?;

        self.client
            .execute(
                &stmt,
                &[
                    &code.as_slice(),
                    &is_contract,
                    &contracts,
                    hash
                ],
            )
            .await
            ?;
    
        Ok(())
    }

    pub async fn read_binary(&self, hash: &Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let code = self.client
            .prepare_typed(
                "select code from public.zephyr_programs WHERE hash = $1",
                &[tokio_postgres::types::Type::BYTEA],
            )
            .await
            .unwrap();

        let rows = self.client.query(&code, &[hash]).await.unwrap();
        let code: Vec<u8> = rows
            .get(0)
            .ok_or(anyhow::Error::msg("Cannot read binary"))?
            .get(0);

        Ok(code)
    }

    /*async fn read_binaries(&self) -> anyhow::Result<Vec<ConstructedZephyrBinary>> {
        let code = self.client
            .prepare_typed(
                "select code, is_contract, contracts from public.zephyr_programs",
                &[],
            )
            .await?;
    
        let rows = self.client.query(&code, &[]).await?;
        let mut binaries = Vec::new();
    
        for row in rows {
            let code: Vec<u8> = row.get(0);
            let is_contract: bool = row.try_get(1).unwrap_or(false);
    
            let contracts = if is_contract {
                Some(row.try_get(2).unwrap_or(vec![]))
            } else {
                None
            };
    
            binaries.push(ConstructedZephyrBinary {
                code,
                is_contract,
                contracts,
            })
        }
    
        Ok(binaries)
    }*/

    pub async fn handle_instructions(&mut self) {
        loop {
            if let Some(received) = self.receiver.recv().await {
                match received.instruction {
                    DbInstruction::SyncWhitelisted => {
                        // reads whitelisted from database and updates self.whitelisted
                    },

                    DbInstruction::UploadBinary(binary) => {
                        let hash = sha2::Sha256::digest(&binary.code).to_vec();
                        if self.whitelisted.contains(&hash) {
                            let _ = self.upload(&hash, &binary.code, binary.contract.unwrap_or(false), binary.contracts.unwrap_or_default()).await;
                            received.callback_send.send(ActorStatus::Uploaded);
                        } else {
                            received.callback_send.send(ActorStatus::NotWhitelisted);
                        }
                    },

                    DbInstruction::RequestBinary(hash) => {
                        if self.whitelisted.contains(&hash) {
                            if let Ok(binary) = self.read_binary(&hash).await {
                                received.callback_send.send(ActorStatus::GotBinary(binary));
                            } else {
                                received.callback_send.send(ActorStatus::FailedToGetBinary);
                            }
                        } else {
                            received.callback_send.send(ActorStatus::NotWhitelisted);
                        }
                    }
                }
            }
        }
    }
}
