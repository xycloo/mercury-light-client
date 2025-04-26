use std::sync::Arc;

use database::{ActorStatus, DbInstruction, DbInstructionWithCallback, PgConnectionActor};
use serverless::{Config, InvokeZephyrFunction};
use tokio::sync::{mpsc, oneshot};
use warp::{reject::Rejection, reply::WithStatus, Filter, http::StatusCode};
use serde::{Deserialize, Serialize};

mod macros;
mod database;
mod serverless;

#[derive(Deserialize, Serialize, Debug)]
struct CodeUploadClient {
    code: Vec<u8>,
    contract: Option<bool>,
    contracts: Option<Vec<String>>,
}

fn with_ctx<T: Send+Sync+Clone>(
    ctx: Arc<T>,
) -> impl Filter<Extract = (Arc<T>,), Error = std::convert::Infallible> + Clone
{
    warp::any().map(move || ctx.clone())
}

async fn get_config() -> Config {
    let project_definition = tokio::fs::read_to_string("./config/mercury.toml").await.unwrap();
    toml::from_str(&project_definition).unwrap()
}


#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel(20);
    let database_sender = Arc::new(tx);
    let config = get_config().await;
    let conn_string = config.database_conn.clone();
    let config_arc = Arc::new(config);
    
    tokio::spawn(async move {
        let mut actor = PgConnectionActor::new(rx, &conn_string).await.expect("cannot connect to database");
        actor.handle_instructions().await;
    });

    let upload_zephyr_code = warp::post()
        .and(warp::path("upload"))
        .and(with_ctx(database_sender.clone()))
        .and(warp::body::json())
        .and_then(
            |
                database_sender: Arc<mpsc::Sender<DbInstructionWithCallback>>,
                sub: CodeUploadClient
            | async move {
                let (tx, rx) = oneshot::channel();
                let _ = database_sender.send(DbInstructionWithCallback::new(DbInstruction::UploadBinary(sub), tx)).await;
                
                let status = rx.await.map_err(|_| "internal error while awaiting receiver");
                
                Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                    serde_json::to_string(&status).unwrap(),
                    StatusCode::CREATED,
                ))
            },
        );
    
    let execute_zephyr_serverless = warp::path!("serverless" / String)
        .and(warp::post())
        .and(with_ctx(database_sender))
        .and(with_ctx(config_arc))
        .and(warp::body::json())
        .and_then(
            |hex, database_sender: Arc<mpsc::Sender<DbInstructionWithCallback>>, config: Arc<Config>, invocation: InvokeZephyrFunction| async move {
                let binary_id = hex::decode(&hex).unwrap();
                let (tx, rx) = oneshot::channel();
                let _ = database_sender.send(DbInstructionWithCallback::new(DbInstruction::RequestBinary(binary_id), tx)).await;
                
                if let Ok(status) = rx.await {
                    match status {
                        ActorStatus::GotBinary(binary) => {
                            // now can execute
                            let input = config.function_from_invocation(invocation, binary);
                            let result = serverless::execute_function(input, &config.executor_binary_path).await.map_err(|_| "failed to execute function");

                            Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                                serde_json::to_string(&result).unwrap(),
                                StatusCode::CREATED,
                            ))
                        }
                        _ => {
                            Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                                serde_json::to_string(&status).unwrap(),
                                StatusCode::CREATED,
                            ))
                        }
                    }
                } else {
                    Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                        "internal error while awaiting receiver".to_string(),
                        StatusCode::CREATED,
                    ))
                }
            },
        );


    let routes = warp::post()
    .and(upload_zephyr_code)
    .or(execute_zephyr_serverless);

    warp::serve(routes).run(([0, 0, 0, 0], 8080)).await;
}
