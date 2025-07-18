use postgres::types::{ToSql, Type};
use retroshade::conversion::{FromScVal, TypeKind};
use retroshade::soroban_env_host::xdr::Limits as RetroshadeLimits;
use retroshade::soroban_env_host::LedgerInfo;
use retroshade::{RetroshadeExportPretty, RetroshadesExecution};
use sha2::Digest;
use std::collections::HashMap;
use std::rc::Rc;
use stellar_xdr::next::{
    FeeBumpTransaction, FeeBumpTransactionInnerTx, GeneralizedTransactionSet, Hash,
    Limits as BridgeLimits, Limits, ReadXdr, Transaction, TransactionEnvelope, TransactionMeta,
    TransactionPhase, TransactionSignaturePayload, TransactionSignaturePayloadTaggedTransaction,
    TxSetComponent, WriteXdr,
};
use tracing::info;
use zephyr::snapshot::{self};

use crate::{get_network_id_from_env, ConstructedZephyrBinary};

mod retro_snapshot {
    use retroshade::soroban_env_host::xdr::Limits as RetroshadeLimits;
    use retroshade::soroban_env_host::xdr::ReadXdr;
    use retroshade::soroban_env_host::xdr::WriteXdr;
    use zephyr::snapshot::raw_endpoint::entry_and_ttl;

    use std::rc::Rc;

    pub struct RetroshadeSnapshot {}

    impl retroshade::soroban_env_host::storage::SnapshotSource for RetroshadeSnapshot {
        fn get(
            &self,
            key: &std::rc::Rc<retroshade::soroban_env_host::xdr::LedgerKey>,
        ) -> Result<
            Option<retroshade::soroban_env_host::storage::EntryWithLiveUntil>,
            retroshade::soroban_env_host::HostError,
        > {
            let xdred = entry_and_ttl(key.as_ref().to_xdr(RetroshadeLimits::none()).unwrap())
                .map_err(|_| retroshade::soroban_env_host::xdr::Error::Invalid)?;
            if let Some(xdr_key) = xdred {
                Ok(Some((
                    Rc::new(retroshade::soroban_env_host::xdr::LedgerEntry::from_xdr(
                        xdr_key.0,
                        RetroshadeLimits::none(),
                    )?),
                    xdr_key.1,
                )))
            } else {
                Ok(None)
            }
        }
    }
}

fn xdr_roundtrip<F, T>(xdr: &F) -> T
where
    F: WriteXdr,
    T: retroshade::soroban_env_host::xdr::ReadXdr,
{
    let xdr_bytes = xdr.to_xdr(BridgeLimits::none()).unwrap();
    T::from_xdr(xdr_bytes, RetroshadeLimits::none()).unwrap()
}

pub(crate) fn txphase_to_sequential_txenvelopes(
    phase: &TransactionPhase,
    envelopes: &mut Vec<TransactionEnvelope>,
) {
    match phase {
        TransactionPhase::V0(phasev0) => {
            for component in phasev0.iter() {
                let TxSetComponent::TxsetCompTxsMaybeDiscountedFee(discounted) = component;
                for envelope in discounted.txs.iter() {
                    envelopes.push(envelope.clone())
                }
            }
        }
        TransactionPhase::V1(phasev1) => {
            for stage in phasev1.execution_stages.to_vec() {
                for thread in stage.0.to_vec() {
                    for envelope in thread.0.to_vec() {
                        envelopes.push(envelope);
                    }
                }
            }
        }
    };
}

async fn get_tx_hash(tx: &Transaction) -> [u8; 32] {
    let tagged_transaction = TransactionSignaturePayloadTaggedTransaction::Tx(tx.clone());

    let payload = TransactionSignaturePayload {
        network_id: Hash(get_network_id_from_env().await),
        tagged_transaction,
    };

    sha2::Sha256::digest(payload.to_xdr(stellar_xdr::next::Limits::none()).unwrap())
        .try_into()
        .unwrap()
}

pub(crate) async fn get_feebumptx_hash(tx: &FeeBumpTransaction) -> [u8; 32] {
    let tagged_transaction = TransactionSignaturePayloadTaggedTransaction::TxFeeBump(tx.clone());

    let payload = TransactionSignaturePayload {
        network_id: Hash(get_network_id_from_env().await),
        tagged_transaction,
    };

    sha2::Sha256::digest(payload.to_xdr(stellar_xdr::next::Limits::none()).unwrap())
        .try_into()
        .unwrap()
}

pub async fn retroshades_main(
    client: &tokio_postgres::Client,
    mercury_contracts: &Vec<ConstructedZephyrBinary>,
    meta: &Vec<u8>,
) {
    let retroshade_effects = execute_retroshades(&mercury_contracts, meta).await;
    info!("retroshade effects {:?}", retroshade_effects);
    if let Ok(retroshade_effects) = retroshade_effects {
        println!("executed retroshades, inserting in db now");

        for (hash, retroshade, tx_hash) in retroshade_effects {
            let result = db_handle_retroshade(client, retroshade, hash, tx_hash).await;
            println!("Retroshades db handling result is {:?}", result);
        }
    }
}

async fn execute_retroshades<'a>(
    mercury_contracts: &'a Vec<ConstructedZephyrBinary>,
    meta: &Vec<u8>,
) -> anyhow::Result<Vec<([u8; 16], RetroshadeExportPretty, [u8; 32])>> {
    let mut retroshade_effects_with_hash = Vec::new();
    info!("Executing {} retroshades", mercury_contracts.len(),);

    for contract in mercury_contracts {
        let mercury_contracts = vec![contract];

        let codes: Vec<_> = mercury_contracts
            .iter()
            .map(|contract| &contract.code)
            .collect();

        let mut mercury_contracts_hashmap: HashMap<
            retroshade::soroban_env_host::xdr::Hash,
            &'a [u8],
        > = HashMap::new();

        for (index, contract) in mercury_contracts.iter().enumerate() {
            if let Some(contracts) = &contract.contracts {
                for id in contracts {
                    let id = stellar_strkey::Contract::from_string(id);

                    if let Ok(id) = id {
                        mercury_contracts_hashmap
                            .insert(retroshade::soroban_env_host::xdr::Hash(id.0), codes[index]);
                    }
                }
            }
        }

        let ledger_info = {
            let mut ledger_info = LedgerInfo::default();
            ledger_info.protocol_version = 22;
            let ledger_from_state = snapshot::snapshot_utils::get_current_ledger_sequence();
            ledger_info.sequence_number = ledger_from_state.0 as u32;
            ledger_info.timestamp = ledger_from_state.1 as u64;
            ledger_info.network_id = get_network_id_from_env().await;
            ledger_info.max_entry_ttl = 3110400;
            ledger_info
        };

        let meta = { soroban_env_host::xdr::LedgerCloseMeta::from_xdr(meta, Limits::none())? };

        match meta {
            stellar_xdr::next::LedgerCloseMeta::V1(v1) => {
                let processing = v1.tx_processing.to_vec();

                let GeneralizedTransactionSet::V1(txsetv1) = v1.tx_set;
                let mut envelopes = Vec::new();

                for phase in txsetv1.phases.to_vec() {
                    txphase_to_sequential_txenvelopes(&phase, &mut envelopes);
                }

                for transaction_envelope in envelopes {
                    match transaction_envelope {
                        TransactionEnvelope::Tx(v1) => {
                            let tx_hash = get_tx_hash(&v1.tx).await;
                            let mut associated_meta = None;

                            for txmeta in &processing {
                                if let TransactionMeta::V3(v3) = &txmeta.tx_apply_processing {
                                    if tx_hash == txmeta.result.transaction_hash.0 {
                                        associated_meta = Some(v3);
                                    }
                                }
                            }

                            if let Some(associated_meta) = associated_meta {
                                let mut retroshades_execution =
                                    RetroshadesExecution::new(ledger_info.clone());

                                let has_retroshades = retroshades_execution
                                    .build_from_envelope_and_meta(
                                        Box::new(retro_snapshot::RetroshadeSnapshot {}),
                                        xdr_roundtrip(&v1),
                                        xdr_roundtrip(associated_meta),
                                        mercury_contracts_hashmap.clone(),
                                    )
                                    .map_err(|e| anyhow::anyhow!("retroshades error {:?}", e));

                                // is in fact a soroban tx
                                if let Ok(true) = has_retroshades {
                                    let retroshades = retroshades_execution
                                        .retroshade_packed_recording(Rc::new(
                                            retro_snapshot::RetroshadeSnapshot {},
                                        ))
                                        .map_err(|e| anyhow::anyhow!("retroshades error {:?}", e));

                                    if let Ok(retroshades) = retroshades {
                                        println!(
                                            "Adding retroshades effect for contracts {:?}",
                                            contract.contracts
                                        );
                                        let retroshade_hash = md5::compute(codes[0]).0;
                                        retroshade_effects_with_hash.push((
                                            retroshade_hash,
                                            retroshades,
                                            tx_hash,
                                        ))
                                    } else {
                                        println!("Error in retroshades {:?}", retroshades);
                                    }
                                }
                            }
                        }

                        TransactionEnvelope::TxFeeBump(feebump) => {
                            let inner_tx = feebump.tx;
                            let tx_hash = get_feebumptx_hash(&inner_tx).await;
                            let mut associated_meta = None;

                            for txmeta in &processing {
                                if let TransactionMeta::V3(v3) = &txmeta.tx_apply_processing {
                                    if tx_hash == txmeta.result.transaction_hash.0 {
                                        associated_meta = Some(v3);
                                    }
                                }
                            }

                            if let Some(associated_meta) = associated_meta {
                                let FeeBumpTransactionInnerTx::Tx(v1) = inner_tx.inner_tx;

                                let mut retroshades_execution =
                                    RetroshadesExecution::new(ledger_info.clone());

                                let has_retroshades = retroshades_execution
                                    .build_from_envelope_and_meta(
                                        Box::new(retro_snapshot::RetroshadeSnapshot {}),
                                        xdr_roundtrip(&v1),
                                        xdr_roundtrip(associated_meta),
                                        mercury_contracts_hashmap.clone(),
                                    )
                                    .map_err(|e| anyhow::anyhow!("retroshades error {:?}", e));

                                if let Ok(true) = has_retroshades {
                                    let retroshades = retroshades_execution
                                        .retroshade_packed_recording(Rc::new(
                                            retro_snapshot::RetroshadeSnapshot {},
                                        ))
                                        .map_err(|e| anyhow::anyhow!("retroshades error {:?}", e));

                                    if let Ok(retroshades) = retroshades {
                                        let retroshade_hash = md5::compute(codes[0]).0;
                                        retroshade_effects_with_hash.push((
                                            retroshade_hash,
                                            retroshades,
                                            tx_hash,
                                        ))
                                    } else {
                                        println!("Error in retroshades {:?}", retroshades);
                                    }
                                }
                            }
                        }

                        _ => (),
                    }
                }
            }
            _ => (),
        };
    }

    let mut retroshades_exports = Vec::new();

    for (binary_hash, effect, tx_hash) in retroshade_effects_with_hash {
        for shade in effect.retroshades {
            retroshades_exports.push((binary_hash, shade, tx_hash))
        }
    }

    Ok(retroshades_exports)
}

pub(crate) async fn db_handle_retroshade(
    client: &tokio_postgres::Client,
    retroshade: RetroshadeExportPretty,
    mercury_contract_hash: [u8; 16],
    tx_hash: [u8; 32],
) -> anyhow::Result<()> {
    let create_table_query = build_create_table_query(&retroshade, mercury_contract_hash)?;
    client.execute(&create_table_query, &[]).await?;

    let (insert_query, params) =
        build_insert_query_and_params(&retroshade, mercury_contract_hash, tx_hash)?;

    let param_refs: Vec<&(dyn ToSql + Sync)> =
        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

    client.execute(&insert_query, &param_refs[..]).await?;

    Ok(())
}

fn build_create_table_query(
    retroshade: &RetroshadeExportPretty,
    mercury_contract_hash: [u8; 16],
) -> anyhow::Result<String> {
    let target = format!(
        "{}{}",
        retroshade.target,
        hex::encode(mercury_contract_hash)
    );

    let mut query = format!(
        "CREATE TABLE IF NOT EXISTS retroshade.{} (contract_id TEXT, transaction TEXT",
        target
    );

    for event in &retroshade.event {
        let column_type = match event.value.dbtype {
            Type::BOOL => "BOOLEAN",
            Type::BOOL_ARRAY => "BOOLEAN[]",
            Type::VOID => "TEXT",
            Type::INT4 => "INTEGER",
            Type::INT8 => "BIGINT",
            Type::NUMERIC => "DOUBLE PRECISION",
            Type::NUMERIC_ARRAY => "DOUBLE PRECISION[]",
            Type::BYTEA => "BYTEA",
            Type::TEXT => "TEXT",
            Type::TEXT_ARRAY => "TEXT[]",
            Type::JSON => "JSONB",
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported type for column {}",
                    event.name
                ))
            }
        };
        query.push_str(&format!(", \"{}\" {}", event.name, column_type));
    }

    query.push(')');
    Ok(query)
}

fn build_insert_query_and_params(
    retroshade: &RetroshadeExportPretty,
    mercury_contract_hash: [u8; 16],
    tx_hash: [u8; 32],
) -> anyhow::Result<(String, Vec<FromScVal>)> {
    let target = format!(
        "{}{}",
        retroshade.target,
        hex::encode(mercury_contract_hash)
    );

    let mut columns = vec!["contract_id", "transaction"];
    let mut placeholders = Vec::new();
    let mut params: Vec<FromScVal> = Vec::new();

    params.push(FromScVal {
        kind: TypeKind::Text(retroshade.contract_id.clone()),
        dbtype: Type::TEXT,
    });

    params.push(FromScVal {
        kind: TypeKind::Text(hex::encode(tx_hash)),
        dbtype: Type::TEXT,
    });

    placeholders.push("$1".to_string());
    placeholders.push("$2".to_string());

    for (i, event) in retroshade.event.iter().enumerate() {
        columns.push(&event.name);
        placeholders.push(format!("${}", i + 3));

        params.push(event.value.clone());
    }

    let query = format!(
        "INSERT INTO retroshade.{} ({}) VALUES ({})",
        target,
        columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders.join(", "),
    );

    Ok((query, params))
}
