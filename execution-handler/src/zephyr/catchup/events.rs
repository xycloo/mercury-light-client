use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};

mod mercury;

#[async_trait::async_trait]
pub trait EventSource<EI: EventInspector> {
    async fn events_after_ledger_by_topics(
        &self,
        contracts_ids: &[String],
        topic1s: &Vec<String>,
        topic2s: &Vec<String>,
        topic3s: &Vec<String>,
        topic4s: &Vec<String>,
        ledger: i64,
    ) -> anyhow::Result<EI>;
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Ledger {
    pub close_time: i64,
    pub sequence: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TxInfo {
    pub tx_hash: String,
    pub ledger_info: Ledger,
    pub result_xdr: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EventNode {
    pub tx_info: TxInfo,
    pub contract_id: String,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
    pub topic4: Option<String>,
    pub data: String,
}

pub trait EventInspector {
    fn all_by_ledger(&self) -> BTreeMap<i64, (i64, Vec<EventNode>)>;
}
