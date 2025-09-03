mod database;
use anyhow::Result;
use base64::prelude::*;
use futures::StreamExt;
use postgres::NoTls;
use reqwest::{
    header::{HeaderMap, HeaderName},
    RequestBuilder, Response,
};
use retroshade::soroban_env_host::xdr::{
    AccountEntryExt, AccountEntryExtensionV1, AccountEntryExtensionV1Ext, LedgerEntry, Limits,
    ReadXdr, ScAddress, ScVal, WriteXdr,
};
use retroshade_handler::retroshades_main;
use rs_zephyr_common::{
    http::{AgnosticRequest, Method},
    Account, ContractDataEntry,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, str::FromStr, time::Duration};
use tokio::{
    fs,
    process::Command,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_postgres::Client;
use tokio_tungstenite::connect_async;
use url::Url;
use zephyr::{db::ledger::LedgerStateRead, ZephyrStandard};

mod retroshade_handler;

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
    pub is_retroshade: bool,
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
/*
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
            tracing::info!("failed to open connection to databased from ledgerread");
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
}*/

async fn read_binaries(client: &Client) -> Result<Vec<ConstructedZephyrBinary>> {
    let code = client
        .prepare_typed(
            "select code, is_retroshade, contracts from public.zephyr_programs",
            &[],
        )
        .await?;

    let rows = client.query(&code, &[]).await?;
    let mut binaries = Vec::new();

    for row in rows {
        let code: Vec<u8> = row.get(0);
        let is_retroshade: bool = row.try_get(1).unwrap_or(false);

        let contracts = if is_retroshade {
            Some(row.try_get(2).unwrap_or(vec![]))
        } else {
            None
        };

        binaries.push(ConstructedZephyrBinary {
            user_id: 0,
            code,
            running: true,
            is_retroshade,
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
    tracing::info!("Connected to the server.");
    let (_, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match message {
            Ok(msg) if msg.is_text() => {
                let text = msg.to_text().unwrap();

                match serde_json::from_str::<SyncRequest>(text) {
                    Ok(sync_req) => {
                        tracing::info!(
                            "Received SyncRequest with meta length: {} bytes",
                            sync_req.meta.len(),
                        );

                        let _ = tx.send(sync_req);
                    }
                    Err(e) => {
                        tracing::info!("Failed to parse SyncRequest: {:?}", e);
                    }
                }
            }
            Ok(msg) if msg.is_close() => {
                tracing::info!("Server closed the connection.");
                break;
            }
            Ok(_) => {
                //
            }
            Err(e) => {
                tracing::info!("Error receiving message: {:?}", e);
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
                tracing::info!("Postgres connection error: {}", e);
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

impl MercuryLight {
    async fn meta_executor(&mut self) {
        let client = &self.db.client;

        let (_constructed, mercury_contracts) = {
            let binaries = read_binaries(&client).await;
            if let Ok(constructed_binaries) = binaries {
                let mut zephyr_programs = Vec::new();
                let mut mercury_contracts = Vec::new();

                for program in constructed_binaries {
                    if program.is_retroshade {
                        mercury_contracts.push(program.clone())
                    } else {
                        zephyr_programs.push(program.clone())
                    }
                }

                let mut running_programs = Vec::new();
                for program in zephyr_programs {
                    if program.running {
                        running_programs.push(program)
                    }
                }

                (running_programs, mercury_contracts)
            } else {
                tracing::warn!("No zephyr binaries found: {:?}", binaries);
                (vec![], vec![])
            }
        };

        // NB: subprocess exec makes it simpler to control the constraints.
        while let Some(data) = self.meta_receiver.recv().await {
            log::info!(target: "info", "Preparing {} retroshades programs to run on ledger", mercury_contracts.len());

            retroshades_main(&client, &mercury_contracts, &data.meta).await;
            //spawn_programs(constructed.clone(), data.meta).await;
        }
    }
}

/*async fn spawn_programs(constructed: Vec<ConstructedZephyrBinary>, meta: Vec<u8>) {
    for binary in constructed {
        let function = FInput {
            associated_data: meta.clone(),
            user_id: 0,
            network_id: get_network_id_from_env().await,
            fname: "on_close".into(),
            binary: BinaryType::Code(binary.code.clone()),
        };

        let meta = {
            soroban_env_host::xdr::LedgerCloseMeta::from_xdr(meta.clone(), Limits::none())
        };

        if meta.is_err() {
            log::error!("Error while unsafely converting ledger close metas on jobs spawn");
            return;
        }

        tracing::info!("\nspawning binary\n");

        tracing::info!("\nexecuting individual vm\n");
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
*/

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
        tracing::info!("successfully called zephyr function")
    } else {
        tracing::info!("Zephyr function error: {:?}", output)
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<SyncRequest>();

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
