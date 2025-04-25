use warp::Filter;
use serde::{Deserialize, Serialize};
use deadpool_postgres::{Config as PgConfig, Pool};
use tokio_postgres::NoTls;
use sha2::{Sha256, Digest};

#[derive(Deserialize)]
struct UploadRequest {
    user_id: i64,
    #[serde(with = "serde_bytes")]     // so bytes come through JSON as base64 automatically
    code: Vec<u8>,
    is_contract: bool,
    contracts: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct ExecuteRequest {
    mode: String,     // e.g. "catchup" or "function"
    // ... any extra params ...
}

#[derive(Deserialize, Serialize, Debug)]
struct CodeUploadClient {
    code: Option<Vec<u8>>,
    contract: Option<bool>,
    contracts: Option<Vec<String>>,
}

fn with_db(pool: Pool) -> impl Filter<Extract = (Pool,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || pool.clone())
}

pub async fn insert_zephyr_code<'a>(
    code: &Vec<u8>,
    is_contract: bool,
    contracts: Vec<String>,
) -> Result<(), Error> {
    let postgres_args: String = env::var("INGESTOR_DB").unwrap();

    let (client, connection) = tokio_postgres::connect(&postgres_args, NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    /*
    let delete_stmt = client
        .prepare_typed(
            "DELETE FROM public.zephyr_programs WHERE user_id = $1 AND project_name = $2",
            &[
                tokio_postgres::types::Type::INT4,
                tokio_postgres::types::Type::TEXT,
            ],
        )
        .await
        .unwrap();

    client
        .execute(&delete_stmt, &[&user, &project_name])
        .await
        .unwrap();
    */

    let stmt = client
        .prepare_typed(
            "INSERT INTO public.zephyr_programs (code, is_contract, contracts) VALUES ($1, $2, $3)",
            &[
                tokio_postgres::types::Type::BYTEA,
                tokio_postgres::types::Type::BOOL,
                tokio_postgres::types::Type::TEXT_ARRAY,
            ],
        )
        .await
        .unwrap();

    client
        .execute(
            &stmt,
            &[
                &code.as_slice(),
                &is_contract,
                &contracts,
            ],
        )
        .await
        .unwrap();

    Ok(())
}

async fn upload(
    code: &Vec<u8>,
    is_contract: bool,
    contracts: Vec<String>,
) -> Result<(), crate::Error> {

    let mut config = wasmi::Config::default();
    let stack_limits = StackLimits::new(
        MIN_VALUE_STACK_HEIGHT,
        MAX_VALUE_STACK_HEIGHT,
        MAX_RECURSION_DEPTH,
    )
    .map_err(|_| Error::InvalidWASMCode)?;

    // TODO: decide which post-mvp features to override.
    // For now we use wasmtime's defaults.
    config.consume_fuel(true);
    config.set_stack_limits(stack_limits);
    config.compilation_mode(wasmi::CompilationMode::Lazy);

    let engine = Engine::new(&config);
    Module::validate(&engine, &code).map_err(|_| Error::InvalidWASMCode)?;

    database::insert_zephyr_code(
        code,
        is_contract,
        contracts,
    )
    .await?;

    Ok(())
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<UserLogPair>();
    let arc_tx = Arc::new(tx.clone());

    let pg_cfg: PgConfig = "./config/database.toml".parse().unwrap();
    let pg_pool: Pool = pg_cfg.create_pool(NoTls).unwrap();

    // check the upload function: start from there
    // understand rhe best way to start the warp server 
    let upload_zephyr_code = warp::post()
        .and(warp::path("zephyr_upload"))
        .and(with_tx(arc_tx.clone()))
        .and(with_db(pg_pool.clone()))
        .and(warp::body::json())
        .and_then(
            |logger: Arc<UnboundedSender<UserLogPair>>,
             mut sub: CodeUploadClient| async move {
                let upload = upload(
                    &sub.code.unwrap(),
                    sub.contract.unwrap_or(false),
                    sub.contracts.unwrap_or(vec![]),
                )
                .await;

                    if let Err(_) = upload {
                        return Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                            "Cannot force replace: use force_replace flag".to_string(),
                            StatusCode::BAD_REQUEST,
                        ));
                    }

                    logger.send(UserLogPair {
                        log: MercuryLog {
                            level: LogLevel::Debug,
                            message: "Uploaded new Zephyr binary".into(),
                            data: None,
                        },
                    });

                    Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                        "true".to_string(),
                        StatusCode::CREATED,
                    ))
            },
        );


        /*
    // 3) Define /execute route
    let execute = warp::post()
        .and(warp::path("execute"))
        */
        
        let routes = upload.with(warp::log("zephyr_uploader"));

        /*
    let routes = upload.or(execute)
        .with(warp::log("zephyr_uploader"));

        */
    warp::serve(routes).run(([0, 0, 0, 0], 8080)).await;
}
