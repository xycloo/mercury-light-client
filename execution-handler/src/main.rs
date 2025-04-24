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

#[tokio::main]
async fn main() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<UserLogPair>();
    let arc_tx = Arc::new(tx.clone());

    let pg_cfg: PgConfig = "./config/database.toml".parse().unwrap();
    let pg_pool: Pool = pg_cfg.create_pool(NoTls).unwrap();

    // check the upload function: start from there
    // understand rhe best way to start the warp server 
    let upload = warp::post()
        .and(warp::path("zephyr_upload"))
        .and(with_tx(arc_tx.clone()))
        .and(with_db(pg_pool.clone()))
        .and(warp::body::json())
        .and_then(
            |logger: Arc<UnboundedSender<UserLogPair>>,
             mut sub: CodeUploadClient| async move {
                if let Ok(id) = authed {
                    let program = ZephyrClient::new(id as i32);
                    let upload = program.upload(&sub.code.unwrap(), sub.force_replace).await;

                    if let Err(_) = upload {
                        return Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                            "Cannot force replace: use force_replace flag".to_string(),
                            StatusCode::BAD_REQUEST,
                        ));
                    }

                    logger.send(UserLogPair {
                        user: id as i64,
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
                } else {
                    println!("non authed");
                    Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                        "Invalid JWT Auth".to_string(),
                        StatusCode::UNAUTHORIZED,
                    ))
                }
            },
        );


        /*
    // 3) Define /execute route
    let execute = warp::post()
        .and(warp::path("execute"))
        */
        
    // 4) Combine and run
    let routes = upload.or(execute)
        .with(warp::log("zephyr_uploader"));

    warp::serve(routes).run(([0, 0, 0, 0], 8080)).await;
}

// Helper to inject the DB pool
fn with_db(pool: Pool) -> impl Filter<Extract = (Pool,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || pool.clone())
}
