use std::sync::Arc;

use database::{ActorStatus, DbInstruction, DbInstructionWithCallback, PgConnectionActor};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use warp::{http::StatusCode, reject::Rejection, reply::WithStatus, Filter};
use zephyr::{
    catchup::GlobalCatchups,
    serverless::{Config, InvokeZephyrFunction},
};

mod database;
mod macros;
mod zephyr;

#[derive(Deserialize, Serialize, Debug)]
struct CodeUploadClient {
    code: Vec<u8>,
    contract: Option<bool>,
    contracts: Option<Vec<String>>,
}

fn with_ctx<T: Send + Sync + Clone>(
    ctx: Arc<T>,
) -> impl Filter<Extract = (Arc<T>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || ctx.clone())
}

async fn get_config() -> Config {
    let project_definition = tokio::fs::read_to_string("./config/mercury.toml")
        .await
        .unwrap();
    toml::from_str(&project_definition).unwrap()
}

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel(20);
    let (status_tx, status_rx) = mpsc::channel(20);
    let (catchup_tx, catchup_rx) = mpsc::channel(20);

    let status_sender = Arc::new(status_tx);
    let catchup_sender = Arc::new(catchup_tx);
    let database_sender = Arc::new(tx.clone());
    let config = get_config().await;
    let config_arc = Arc::new(config.clone());
    let mercury_graphql_endpoint = config.graphql_endpoint.clone();
    let mercury_jwt = config.jwt.clone();
    let conn_string = config.database_conn.clone();

    tokio::spawn(async move {
        let mut actor = PgConnectionActor::new(rx, &conn_string)
            .await
            .expect("cannot connect to database");
        actor.handle_instructions().await;
    });

    let config = config.clone();
    tokio::spawn(async move {
        let mercury_es = zephyr::catchup::events::mercury::MercuryEventsSource::new(
            mercury_graphql_endpoint,
            mercury_jwt,
        );
        let mut global_catchup =
            GlobalCatchups::new(config, Some(mercury_es), catchup_rx, status_rx, tx.clone());

        global_catchup.consume().await;
    });

    let upload_zephyr_code = warp::post()
        .and(warp::path("upload"))
        .and(with_ctx(database_sender.clone()))
        .and(warp::body::json())
        .and_then(
            |database_sender: Arc<mpsc::Sender<DbInstructionWithCallback>>,
             sub: CodeUploadClient| async move {
                let (tx, rx) = oneshot::channel();
                let _ = database_sender
                    .send(DbInstructionWithCallback::new(
                        DbInstruction::UploadBinary(sub),
                        tx,
                    ))
                    .await;

                let status = rx
                    .await
                    .map_err(|_| "internal error while awaiting receiver");

                Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                    serde_json::to_string(&status).unwrap(),
                    StatusCode::CREATED,
                ))
            },
        );

    let sync_whitelist = warp::post()
        .and(warp::path("sync_whitelist"))
        .and(with_ctx(database_sender.clone()))
        .and_then(
            |database_sender: Arc<mpsc::Sender<DbInstructionWithCallback>>| async move {
                let (tx, rx) = oneshot::channel();
                // fire off the instruction
                let _ = database_sender
                    .send(DbInstructionWithCallback::new(
                        DbInstruction::SyncWhitelisted,
                        tx,
                    ))
                    .await;
                // await the actor’s reply
                // implement better error handling also here
                let status = rx.await.map_err(|_| "sync whitelist error");

                Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                    serde_json::to_string(&status).unwrap(),
                    StatusCode::OK,
                ))
            },
        );

    let execute_zephyr_serverless = warp::path!("serverless" / String)
        .and(warp::post())
        .and(with_ctx(database_sender.clone()))
        .and(with_ctx(config_arc.clone()))
        .and(warp::body::json())
        .and_then(
            |hex,
             database_sender: Arc<mpsc::Sender<DbInstructionWithCallback>>,
             config: Arc<Config>,
             invocation: InvokeZephyrFunction| async move {
                let binary_id = hex::decode(&hex).unwrap();
                let (tx, rx) = oneshot::channel();
                let _ = database_sender
                    .send(DbInstructionWithCallback::new(
                        DbInstruction::RequestBinary(binary_id),
                        tx,
                    ))
                    .await;

                if let Ok(status) = rx.await {
                    match status {
                        ActorStatus::GotBinary(binary) => {
                            // now can execute
                            let input = config.function_from_invocation(invocation, binary);
                            let result = zephyr::serverless::execute_function(
                                input,
                                &config.executor_binary_path,
                            )
                            .await
                            .map_err(|_| "failed to execute function");

                            Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                                serde_json::to_string(&result).unwrap(),
                                StatusCode::CREATED,
                            ))
                        }
                        _ => Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                            serde_json::to_string(&status).unwrap(),
                            StatusCode::CREATED,
                        )),
                    }
                } else {
                    Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                        "internal error while awaiting receiver".to_string(),
                        StatusCode::CREATED,
                    ))
                }
            },
        );

    let zephyr_catchup = warp::path!("zephyr" / "catchup" / String)
        .and(warp::post())
        .and(with_ctx(database_sender))
        .and(with_ctx(config_arc))
        .and(warp::body::json())
        .and_then(
            |hex,
             database_sender: Arc<mpsc::Sender<DbInstructionWithCallback>>,
             config: Arc<Config>,
             invocation: InvokeZephyrFunction| async move {
                let binary_id = hex::decode(&hex).unwrap();
                let (tx, rx) = oneshot::channel();
                let _ = database_sender
                    .send(DbInstructionWithCallback::new(
                        DbInstruction::RequestBinary(binary_id),
                        tx,
                    ))
                    .await;

                if let Ok(status) = rx.await {
                    match status {
                        ActorStatus::GotBinary(binary) => {
                            // now can execute
                            let input = config.function_from_invocation(invocation, binary);
                            let result = zephyr::serverless::execute_function(
                                input,
                                &config.executor_binary_path,
                            )
                            .await
                            .map_err(|_| "failed to execute function");

                            Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                                serde_json::to_string(&result).unwrap(),
                                StatusCode::CREATED,
                            ))
                        }
                        _ => Ok::<WithStatus<String>, Rejection>(warp::reply::with_status(
                            serde_json::to_string(&status).unwrap(),
                            StatusCode::CREATED,
                        )),
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
        .or(sync_whitelist)
        .or(execute_zephyr_serverless)
        .or(zephyr_catchup);

    warp::serve(routes).run(([0, 0, 0, 0], 8080)).await;
}
