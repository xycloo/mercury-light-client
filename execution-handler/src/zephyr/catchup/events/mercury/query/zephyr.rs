use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::zephyr::catchup::events::EventInspector;

pub fn get_query_generic(
    contracts: &[String],
    topic1s: &Vec<String>,
    topic2s: &Vec<String>,
    topic3s: &Vec<String>,
    topic4s: &Vec<String>,
    ledger: Option<i64>,
) -> Request {
    let mut contracts_string = String::from("[");
    for (idx, contract) in contracts.iter().enumerate() {
        if idx == contracts.len() - 1 {
            contracts_string.push_str(&format!("\"{}\"]", contract))
        } else {
            contracts_string.push_str(&format!("\"{}\", ", contract))
        }
    }

    let t1_value = if !topic1s.is_empty() {
        let mut string = String::from(" , t1: [");
        for (idx, topic) in topic1s.iter().enumerate() {
            if idx == topic1s.len() - 1 {
                string.push_str(&format!("\"{topic}\""));
            } else {
                string.push_str(&format!("\"{topic}\", "));
            }
        }
        string.push(']');
        string
    } else {
        String::new()
    };

    let t2_value = if !topic2s.is_empty() {
        let mut string = String::from(" , t2: [");
        for (idx, topic) in topic2s.iter().enumerate() {
            if idx == topic2s.len() - 1 {
                string.push_str(&format!("\"{topic}\""));
            } else {
                string.push_str(&format!("\"{topic}\", "));
            }
        }
        string.push(']');
        string
    } else {
        String::new()
    };

    let t3_value = if !topic3s.is_empty() {
        let mut string = String::from(" , t3: [");
        for (idx, topic) in topic3s.iter().enumerate() {
            if idx == topic3s.len() - 1 {
                string.push_str(&format!("\"{topic}\""));
            } else {
                string.push_str(&format!("\"{topic}\", "));
            }
        }
        string.push(']');
        string
    } else {
        String::new()
    };

    let t4_value = if !topic4s.is_empty() {
        let mut string = String::from(" , t4: [");
        for (idx, topic) in topic4s.iter().enumerate() {
            if idx == topic4s.len() - 1 {
                string.push_str(&format!("\"{topic}\""));
            } else {
                string.push_str(&format!("\"{topic}\", "));
            }
        }
        string.push(']');
        string
    } else {
        String::new()
    };

    let ledger_value = if let Some(ledger) = ledger {
        format!(" , ledgerValue: {ledger}")
    } else {
        Default::default()
    };

    let query = format!(
        "
query Test {{
    eventByContractIdsAndTopics(ids: {contracts_string}{t1_value}{t2_value}{t3_value}{t4_value}{ledger_value}) {{
        nodes {{
        txInfoByTx {{
            txHash,
            ledgerByLedger {{
                closeTime,
                sequence
            }}
        }}
        contractId,
        topic1,
        topic2,
        topic3,
        topic4,
        data
        }}
    }}
}}
    ",
    );

    Request { query }
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub query: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Ledger {
    pub closeTime: i64,
    pub sequence: i64,
}

impl Into<crate::zephyr::catchup::events::Ledger> for Ledger {
    fn into(self) -> crate::zephyr::catchup::events::Ledger {
        crate::zephyr::catchup::events::Ledger {
            close_time: self.closeTime,
            sequence: self.sequence,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TxInfo {
    pub txHash: String,
    pub ledgerByLedger: Ledger,
    pub resultXdr: Option<String>,
}

impl Into<crate::zephyr::catchup::events::TxInfo> for TxInfo {
    fn into(self) -> crate::zephyr::catchup::events::TxInfo {
        crate::zephyr::catchup::events::TxInfo {
            tx_hash: self.txHash,
            ledger_info: self.ledgerByLedger.into(),
            result_xdr: self.resultXdr,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EventNode {
    pub txInfoByTx: TxInfo,
    pub contractId: String,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
    pub topic4: Option<String>,
    pub data: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EventByContractId {
    pub nodes: Vec<EventNode>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Data {
    pub eventByContractIds: EventByContractId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Response {
    pub data: Data,
}

impl EventInspector for Response {
    fn all_by_ledger(
        &self,
    ) -> BTreeMap<i64, (i64, Vec<crate::zephyr::catchup::events::EventNode>)> {
        let mut all_events_by_ledger: BTreeMap<
            i64,
            (i64, Vec<crate::zephyr::catchup::events::EventNode>),
        > = BTreeMap::new();

        for event in self.data.eventByContractIds.nodes.clone() {
            let seq = event.txInfoByTx.ledgerByLedger.sequence;
            let time = event.txInfoByTx.ledgerByLedger.closeTime;

            if all_events_by_ledger.contains_key(&seq) {
                let mut other_events: Vec<crate::zephyr::catchup::events::EventNode> =
                    all_events_by_ledger
                        .get(&seq)
                        .unwrap()
                        .1
                        .to_vec()
                        .iter()
                        .map(|e| e.clone().into())
                        .collect();
                other_events.push(event.into());
                all_events_by_ledger.insert(seq, (time, other_events));
            } else {
                all_events_by_ledger.insert(seq, (time, vec![event.into()]));
            }
        }

        all_events_by_ledger
    }
}

impl Into<crate::zephyr::catchup::events::EventNode> for EventNode {
    fn into(self) -> crate::zephyr::catchup::events::EventNode {
        crate::zephyr::catchup::events::EventNode {
            tx_info: self.txInfoByTx.into(),
            contract_id: self.contractId,
            topic1: self.topic1,
            topic2: self.topic2,
            topic3: self.topic3,
            topic4: self.topic4,
            data: self.data,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataAfterLedger {
    #[serde(rename = "eventByContractIdsAndTopics")]
    pub eventByContractIds: EventByContractId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ResponseAfterLedger {
    pub data: DataAfterLedger,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RetroshadesNode {
    pub txInfoByTx: TxInfo,
    pub v1Envelope: Option<String>,
    pub v3Meta: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RetroshadesByContractId {
    pub nodes: Vec<RetroshadesNode>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RetroshdesAfterLedger {
    #[serde(rename = "sorobanTxByContractIdsAndFnames")]
    pub sorobanTxByContractIdsAndFnames: RetroshadesByContractId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RetroshadesResponse {
    pub data: RetroshdesAfterLedger,
}

pub fn get_soroban_txs(contracts: &[String], fnames: &Vec<String>, ledger: Option<i64>) -> Request {
    let mut contracts_string = String::from("[");
    for (idx, contract) in contracts.iter().enumerate() {
        if idx == contracts.len() - 1 {
            contracts_string.push_str(&format!("\"{}\"]", contract))
        } else {
            contracts_string.push_str(&format!("\"{}\", ", contract))
        }
    }

    let t1_value = if !fnames.is_empty() {
        let mut string = String::from(" , fnames: [");
        for (idx, topic) in fnames.iter().enumerate() {
            if idx == fnames.len() - 1 {
                string.push_str(&format!("\"{topic}\""));
            } else {
                string.push_str(&format!("\"{topic}\", "));
            }
        }
        string.push(']');
        string
    } else {
        String::new()
    };

    let ledger_value = if let Some(ledger) = ledger {
        format!(" , ledgervalue: {ledger}")
    } else {
        Default::default()
    };

    let query = format!(
        "
query Test {{
    sorobanTxByContractIdsAndFnames(ids: {contracts_string}{t1_value}{ledger_value}) {{
        nodes {{
        txInfoByTx {{
            txHash,
            resultXdr,
            ledgerByLedger {{
                closeTime,
                sequence
            }}
        }}
        v1Envelope,
        v3Meta
        }}
    }}
}}
    ",
    );

    Request { query }
}
