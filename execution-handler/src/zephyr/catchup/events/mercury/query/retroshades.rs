use serde::{Deserialize, Serialize};

use super::zephyr::{Request, TxInfo};

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
