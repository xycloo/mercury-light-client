use query::zephyr::{get_query_generic, Data, Response, ResponseAfterLedger};

use super::{EventInspector, EventSource};

pub mod query;

#[derive(Clone)]
pub struct MercuryEventsSource {
    graphql_endpoint: String,
    jwt: String,
}

impl MercuryEventsSource {
    pub fn new(graphql_endpoint: String, jwt: String) -> Self {
        Self {
            graphql_endpoint,
            jwt,
        }
    }
}

#[async_trait::async_trait]
impl EventSource<Response> for MercuryEventsSource {
    async fn events_after_ledger_by_topics(
        &self,
        contracts_ids: &[String],
        topic1s: &Vec<String>,
        topic2s: &Vec<String>,
        topic3s: &Vec<String>,
        topic4s: &Vec<String>,
        ledger: i64,
    ) -> anyhow::Result<Response> {
        let jwt = &self.jwt;
        let client = reqwest::Client::new();

        let res = client
            .post(&self.graphql_endpoint)
            .bearer_auth(jwt)
            .json(&get_query_generic(
                contracts_ids,
                topic1s,
                topic2s,
                topic3s,
                topic4s,
                Some(ledger),
            ))
            .send()
            .await?;

        let resp: ResponseAfterLedger = res.json().await?;
        let resp = Response {
            data: Data {
                eventByContractIds: resp.data.eventByContractIds,
            },
        };

        Ok(resp)
    }
}
