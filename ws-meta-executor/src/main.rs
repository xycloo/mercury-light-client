mod database;
use self::database::MercuryDatabase;
use ::zephyr::{db::ledger::LedgerStateRead, host::Host, vm::Vm, ZephyrStandard};
use anyhow::Result;
use base64::prelude::*;
use futures::StreamExt;
use multiuser_logging_service::LoggingClient;
use postgres::NoTls;
use reqwest::{
    header::{HeaderMap, HeaderName},
    RequestBuilder, Response,
};
use rs_zephyr_common::{
    http::{AgnosticRequest, Method},
    Account, ContractDataEntry, RelayedMessageRequest,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use soroban_env_host::xdr::{
    AccountEntryExt, AccountEntryExtensionV1, AccountEntryExtensionV1Ext, LedgerEntry, Limits,
    ReadXdr, ScAddress, ScVal, WriteXdr,
};
use std::{collections::HashMap, env, rc::Rc, str::FromStr, time::Duration};
use tokio::{
    fs,
    process::Command,
    runtime::Handle,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_postgres::Client;
use tokio_tungstenite::connect_async;
use url::Url;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub network: String,
    pub min: Option<u32>,
    pub max: Option<u32>,
    pub frequency: u32,
    pub ws_address: String,
    pub database_conn: String,
    pub executor_binary_path: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SyncRequest {
    meta: Vec<u8>,
    sequence: i64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FInput {
    pub associated_data: Vec<u8>,
    pub binary: BinaryType,
    pub user_id: u64,
    pub network_id: [u8; 32],
    pub fname: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum BinaryType {
    Path(String),
    Code(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct ConstructedZephyrBinary {
    pub user_id: i32,
    pub code: Vec<u8>,
    pub running: bool,
    pub is_contract: bool,
    pub contracts: Option<Vec<String>>,
}

pub struct WebhookJob {
    builder_cloned: Option<RequestBuilder>,
    response: Result<Response, reqwest::Error>,
    request: AgnosticRequest,
}

pub async fn get_config() -> Config {
    let project_definition = tokio::fs::read_to_string("././config/mercury.toml")
        .await
        .unwrap();
    toml::from_str(&project_definition).unwrap()
}

impl WebhookJob {
    fn build_request(request: AgnosticRequest) -> RequestBuilder {
        let client = reqwest::Client::new();
        let mut headers = HeaderMap::new();
        for (k, v) in &request.headers {
            headers.insert(
                HeaderName::from_str(&k).unwrap(),
                v.parse().unwrap_or("invalidheader".parse().unwrap()),
            );
        }

        let builder = match request.method {
            Method::Get => {
                let builder = client.get(&request.url).headers(headers);

                if let Some(body) = &request.body {
                    builder.body(body.clone())
                } else {
                    builder
                }
            }

            Method::Post => {
                let builder = client.post(&request.url).headers(headers);

                if let Some(body) = &request.body {
                    builder.body(body.clone())
                } else {
                    builder
                }
            }
        };
        let builder = builder.timeout(Duration::from_secs(10));

        builder
    }

    fn is_success(&self) -> bool {
        if let Ok(resp) = self.response.as_ref() {
            resp.status().is_success()
        } else {
            false
        }
    }

    fn inspect(&self) -> Option<String> {
        if let Ok(resp) = self.response.as_ref() {
            let inspection = format!(
                "Status: {}\nHeaders: {:?}\nRequest: {:?}",
                resp.status().as_str(),
                resp.headers(),
                self.request
            );
            Some(inspection)
        } else {
            None
        }
    }

    pub async fn handle_retry(&mut self, max_retries: u32) -> Result<u32, String> {
        // Should already be triggered
        // but keeping extra precaution.
        if self.is_success() {
            return Ok(0);
        }

        let mut attempts = 0;
        while attempts < max_retries {
            let backoff_duration = Duration::from_secs(2u64.pow(attempts));
            tokio::time::sleep(backoff_duration).await;

            if let Some(builder) = self.builder_cloned.take() {
                let new_response = builder.send().await;
                self.response = new_response;

                if self.is_success() {
                    return Ok(attempts);
                }
            } else {
                let new_builder = Self::build_request(self.request.clone());
                let new_response = new_builder.send().await;
                self.response = new_response;

                if self.is_success() {
                    return Ok(attempts);
                }
            }

            attempts += 1;
        }

        Err(self.inspect().unwrap_or("Reqwest error".into()))
    }
}

#[derive(Clone)]
pub struct LedgerReader {
    path: String,
}

impl ZephyrStandard for LedgerReader {
    fn zephyr_standard() -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            path: "/tmp/rs_ingestion_temp/stellar.db".into(),
        })
    }
}

impl LedgerStateRead for LedgerReader {
    fn read_contract_data_entry_by_contract_id_and_key(
        &self,
        contract: ScAddress,
        key: ScVal,
    ) -> Option<ContractDataEntry> {
        let conn = rusqlite::Connection::open(&self.path);
        let conn = if let Ok(conn) = conn {
            conn
        } else {
            println!("failed to open connection to databased from ledgerread");
            return None;
        };

        let query_string = format!("SELECT contractid, key, ledgerentry, \"type\", lastmodified FROM contractdata where contractid = ?1 AND key = ?2");

        let stmt = conn.prepare(&query_string);
        let mut stmt = if let Ok(stmt) = stmt {
            stmt
        } else {
            return None;
        };

        let entries = stmt.query_map(
            rusqlite::params![
                contract.to_xdr_base64(Limits::none()).unwrap(),
                key.to_xdr_base64(Limits::none()).unwrap()
            ],
            |row| {
                Ok(ContractDataEntry {
                    contract_id: contract.clone(),
                    key: ScVal::from_xdr_base64(
                        row.get::<usize, String>(1).unwrap(),
                        Limits::none(),
                    )
                    .unwrap(),
                    entry: LedgerEntry::from_xdr_base64(
                        row.get::<usize, String>(2).unwrap(),
                        Limits::none(),
                    )
                    .unwrap(),
                    durability: row.get(3).unwrap(),
                    last_modified: row.get(4).unwrap(),
                })
            },
        );

        let entries = entries
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<ContractDataEntry>>();

        Some(entries[0].clone())
    }

    fn read_contract_data_entries_by_contract_id(
        &self,
        contract: ScAddress,
    ) -> Vec<ContractDataEntry> {
        let conn = rusqlite::Connection::open(&self.path).unwrap();

        let query_string = format!("SELECT contractid, key, ledgerentry, \"type\", lastmodified FROM contractdata where contractid = ?1");

        let mut stmt = conn.prepare(&query_string).unwrap();
        let entries = stmt.query_map(
            rusqlite::params![contract.to_xdr_base64(Limits::none()).unwrap()],
            |row| {
                let entry = ContractDataEntry {
                    contract_id: contract.clone(),
                    key: ScVal::from_xdr_base64(
                        row.get::<usize, String>(1).unwrap(),
                        Limits::none(),
                    )
                    .unwrap(),
                    entry: LedgerEntry::from_xdr_base64(
                        row.get::<usize, String>(2).unwrap(),
                        Limits::none(),
                    )
                    .unwrap(),
                    durability: row.get(3).unwrap(),
                    last_modified: row.get(4).unwrap(),
                };

                Ok(entry)
            },
        );

        entries
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<ContractDataEntry>>()
    }

    fn read_account(&self, account: String) -> Option<Account> {
        let conn = rusqlite::Connection::open(&self.path).unwrap();
        let mut stmt = conn
            .prepare("SELECT * FROM accounts where accountid = ?1")
            .unwrap();

        let account = stmt.query_map(rusqlite::params![account], |row| {
            let (buying_liabilities, selling_liabilities, num_sponsored, num_sponsoring) = {
                let extension = {
                    let b64: String = row.get(12).unwrap_or("".into()); // note, this will get caught by the following line.
                    AccountEntryExt::from_xdr_base64(b64, Limits::none())
                };

                match extension {
                    Ok(AccountEntryExt::V0) => {
                        (row.get(2).unwrap_or(0.0), row.get(3).unwrap_or(0.0), 0, 0)
                    }
                    Ok(AccountEntryExt::V1(AccountEntryExtensionV1 { ext, liabilities })) => {
                        let (sponsored, sponsoring) = match ext {
                            AccountEntryExtensionV1Ext::V0 => (0, 0),
                            AccountEntryExtensionV1Ext::V2(v2_ext) => {
                                (v2_ext.num_sponsored as i32, v2_ext.num_sponsoring as i32)
                            }
                        };

                        (
                            liabilities.buying as f64,
                            liabilities.selling as f64,
                            sponsored,
                            sponsoring,
                        )
                    }
                    Err(_) => (row.get(2).unwrap_or(0.0), row.get(3).unwrap_or(0.0), 0, 0),
                }
            };

            Ok(Account {
                account_id: row.get(0).unwrap(),
                native_balance: row.get(1).unwrap(),
                buying_liabilities,
                selling_liabilities,
                seq_num: row.get(4).unwrap(),
                num_subentries: row.get(5).unwrap(),
                num_sponsored,
                num_sponsoring,
            })
        });

        let last = if let Ok(account) = account {
            if let Some(Ok(account)) = account.last() {
                Some(account)
            } else {
                None
            }
        } else {
            None
        };

        last
    }
}

async fn read_binaries(client: &Client) -> Result<Vec<ConstructedZephyrBinary>> {
    let code = client
        .prepare_typed(
            "select user_id, code, running, is_contract, contracts from public.zephyr_programs",
            &[],
        )
        .await?;

    let rows = client.query(&code, &[]).await?;
    let mut binaries = Vec::new();

    for row in rows {
        let user_id: i32 = row.get(0);
        let code: Vec<u8> = row.get(1);
        let running: bool = match row.try_get(2) {
            Ok(status) => status,
            Err(_) => true,
        };
        let is_contract: bool = row.try_get(3).unwrap_or(false);

        let contracts = if is_contract {
            Some(row.try_get(4).unwrap_or(vec![]))
        } else {
            None
        };

        binaries.push(ConstructedZephyrBinary {
            user_id,
            code,
            running,
            is_contract,
            contracts,
        })
    }

    Ok(binaries)
}

async fn get_network_id_from_env() -> [u8; 32] {
    let project_definition = fs::read_to_string("././config/mercury.toml").await.unwrap();
    let config: Config = toml::from_str(&project_definition).unwrap();

    let mut hasher = Sha256::new();
    hasher.update(&config.network);
    hasher.finalize().as_slice().try_into().unwrap()
}

// receives the metas through the ws server either from mercury's server or your local machine, then sends the message over the unbounded channel
async fn fill_metas(tx: UnboundedSender<SyncRequest>) {
    let project_definition = fs::read_to_string("././config/mercury.toml").await.unwrap();
    let config: Config = toml::from_str(&project_definition).unwrap();
    let url = Url::parse(&config.ws_address).unwrap();
    let (ws_stream, _) = connect_async(url)
        .await
        .expect("Failed to connect to the server");
    println!("Connected to the server.");
    let (_, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match message {
            Ok(msg) if msg.is_text() => {
                let text = msg.to_text().unwrap();

                match serde_json::from_str::<SyncRequest>(text) {
                    Ok(sync_req) => {
                        println!(
                            "Received SyncRequest with meta length: {} bytes and sequence {}",
                            sync_req.meta.len(),
                            sync_req.sequence
                        );

                        let _ = tx.send(sync_req);
                    }
                    Err(e) => {
                        eprintln!("Failed to parse SyncRequest: {:?}", e);
                    }
                }
            }
            Ok(msg) if msg.is_close() => {
                println!("Server closed the connection.");
                break;
            }
            Ok(_) => {
                //
            }
            Err(e) => {
                eprintln!("Error receiving message: {:?}", e);
                break;
            }
        }
    }
}

pub struct MercuryLight {
    db: PgConnection,

    /// Array of cached whitelisted program hashes that are allowed to be executed
    whitelisted: Vec<Vec<u8>>,

    /// Maps each binary hash to the binary.
    binaries: HashMap<Vec<u8>, Vec<u8>>,

    meta_receiver: UnboundedReceiver<SyncRequest>,
}
pub struct PgConnection {
    client: Client,
    // We keep the JoinHandle alive so the connection future isn't dropped.
    _connection_task: JoinHandle<()>,
}

impl PgConnection {
    pub async fn new(conn: &str) -> Result<Self, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::connect(conn, NoTls).await?;

        let connection_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {}", e);
            }
        });

        Ok(Self {
            client,
            _connection_task: connection_task,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }
}

// still need to add retroshades

impl MercuryLight {
    async fn meta_executor(&mut self) {
        let config = get_config().await;
        let conn_string = config.database_conn.clone();

        // let postgres_args: String = env::var("INGESTOR_DB").unwrap();

        let (client, connection) = tokio_postgres::connect(&conn_string, NoTls).await.unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let (constructed, mercury_contracts) = {
            if let Ok(constructed_binaries) = read_binaries(&client).await {
                let mut zephyr_programs = Vec::new();
                let mut mercury_contracts = Vec::new();

                for program in constructed_binaries {
                    if program.is_contract {
                        mercury_contracts.push(program.clone())
                    } else {
                        zephyr_programs.push(program.clone())
                    }
                }

                // log::info!(target: "info", "Preparing {} programs to run on ledger {}", zephyr_programs.len(), self.sequence());
                let mut running_programs = Vec::new();
                for program in zephyr_programs {
                    if program.running {
                        running_programs.push(program)
                    }
                }

                (running_programs, mercury_contracts)
            } else {
                log::warn!("No zephyr binaries found");
                (vec![], vec![])
            }

            // client should be dropped here
        };

        // NB: subprocess exec makes it simpler to control the constraints.
        while let Some(data) = self.meta_receiver.recv().await {
            log::info!(target: "info", "Preparing {} programs to run on ledger {}", constructed.len(), data.sequence); // consoder removing this log later if the .clone() impacts performance
            spawn_programs(constructed.clone(), data.meta).await;
        }
    }
}

async fn spawn_programs(constructed: Vec<ConstructedZephyrBinary>, meta: Vec<u8>) {
    for binary in constructed {
        let function = FInput {
            associated_data: meta.clone(),
            user_id: 0,
            network_id: get_network_id_from_env().await,
            fname: "on_close".into(),
            binary: BinaryType::Code(binary.code.clone()),
        };

        let meta = {
            // use stellar_xdr::next::{Limits as BridgeLimits, WriteXdr};
            soroban_env_host::xdr::LedgerCloseMeta::from_xdr(meta.clone(), Limits::none())
        };

        if meta.is_err() {
            log::error!("Error while unsafely converting ledger close metas on jobs spawn");
            return;
        }

        println!("\nspawning binary\n");

        println!("\nexecuting individual vm\n");
        let execution = execute_function(function).await;

        match execution {
            Ok(_) => {
                log::info!(target: "info", "Successfully executed Zephyr program for user {}", binary.user_id)
            }
            Err(e) => log::error!(
                "Error while executing Zephyr for user {}: {:?}",
                binary.user_id,
                e
            ),
        }
    }
}

async fn execute_function(function: FInput) -> anyhow::Result<()> {
    let config = get_config().await;
    let binary_path = config.executor_binary_path.clone();

    let serialized = bincode::serialize(&function).unwrap();
    let encoded = BASE64_STANDARD.encode(&serialized);
    let child =
        Command::new(std::env::var(binary_path).expect("missing BINARY_PATH from enviornment"))
            .arg(encoded.clone())
            .spawn()
            .unwrap();

    let output = child.wait_with_output().await?;
    if output.status.success() {
        println!("[+] successfully called zephyr function")
    } else {
        println!("Zephyr function error: {:?}", output)
    }

    Ok(())
}

/*
pub(crate) async fn execute_individual_vm(
    handle: Handle,
    meta: soroban_env_host::xdr::LedgerCloseMeta,
    binary: ConstructedZephyrBinary,
    network_id: [u8; 32],
) -> Result<()> {
    let user_id = binary.user_id;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

    let join_handle = handle.spawn_blocking(move || run_vm(meta, &binary.clone(), tx, network_id));

    // note: we probably need a much better story for managing this.
    let _timeout = tokio::time::timeout(Duration::from_secs(5), join_handle).await???;
    //join_handle.await??;

    let _ = handle
        .spawn(async move {
            let mut handles = Vec::new();
            while let Some(message) = rx.recv().await {
                let request = bincode::deserialize::<RelayedMessageRequest>(&message);
                match request {
                    Ok(RelayedMessageRequest::Http(request)) => {
                        let handle = tokio::spawn(async move {
                            let builder = WebhookJob::build_request(request.clone());
                            let builder_cloned = builder.try_clone();

                            let response = builder.send().await;
                            let mut job = WebhookJob {
                                builder_cloned,
                                response,
                                request,
                            };

                            if !job.is_success() {
                                let logging_client = LoggingClient::new();
                                let _ = logging_client
                                    .send_log(
                                        user_id as i64,
                                        multiuser_logging_service::LogLevel::Error,
                                        job.inspect().unwrap_or("Reqwest error".into()),
                                    )
                                    .await;

                                let result = job.handle_retry(3).await;

                                if result.is_ok() {
                                    let _ = logging_client
                                        .send_log(
                                            user_id as i64,
                                            multiuser_logging_service::LogLevel::Warning,
                                            format!(
                                                "Request succeeded after {} attempts",
                                                result.unwrap()
                                            ),
                                        )
                                        .await;
                                } else {
                                    let _ = logging_client
                                        .send_log(
                                            user_id as i64,
                                            multiuser_logging_service::LogLevel::Error,
                                            format!(
                                                "Exceeded max retries, request still failing: {}",
                                                result.err().unwrap()
                                            ),
                                        )
                                        .await;
                                }
                            };
                        });

                        handles.push(handle)
                    }

                    Ok(RelayedMessageRequest::Log(log)) => {
                        let mut map = HashMap::new();
                        map.insert("serialized", bincode::serialize(&log).unwrap());

                        let client = reqwest::Client::new();
                        let _ = client
                            .post(format!("http://127.0.0.1:8082/log/{}", user_id))
                            .json(&map)
                            .send()
                            .await;
                    }

                    _ => {}
                }
            }

            for handle in handles {
                let result = handle.await;
            }
        })
        .await;

    Ok(())
}

fn run_vm(
    meta: soroban_env_host::xdr::LedgerCloseMeta,
    binary: &ConstructedZephyrBinary,
    tx: UnboundedSender<Vec<u8>>,
    network_id: [u8; 32],
) -> Result<()> {
    let mut host: Host<MercuryDatabase, LedgerReader> =
        Host::from_id(binary.user_id as i64, network_id)?;

    host.add_transmitter(tx);
    host.add_ledger_close_meta(meta.to_xdr(soroban_env_host::xdr::Limits::none()).unwrap())?;

    let start = std::time::Instant::now();
    let vm = Vm::new(&host, &binary.code)?;

    log::info!(target: "zephyr", "Time elapsed after instantiation: {:?}", start.elapsed());

    host.load_context(Rc::downgrade(&vm)).unwrap();
    let call = vm.metered_function_call(&host, "on_close");

    log::info!(target: "zephyr", "Time elapsed after execution for user {}: {:?}", binary.user_id, start.elapsed());

    call.unwrap_or("Unreachable operation executed".into());

    Ok(())
}
    */

#[tokio::main]
async fn main() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<SyncRequest>();

    let config = get_config().await;
    let mut light = MercuryLight {
        db: PgConnection::new(&config.database_conn).await.unwrap(),
        whitelisted: vec![],
        binaries: HashMap::new(),
        meta_receiver: rx,
    };

    let fill_metas = tokio::spawn(async move { fill_metas(tx).await });
    let spawn_executor = tokio::spawn(async move { light.meta_executor().await });

    let _ = tokio::join!(fill_metas, spawn_executor);
}
