use crate::{database::DbInstructionWithCallback, make_continue};
use events::{EventInspector, EventSource};
use soroban_env_host::xdr::{Limits, WriteXdr};
use std::marker::PhantomData;
use tokio::sync::mpsc;
use tracing::{debug, info};
use types::{
    CatchupFunction, ExecuteCatchupRequest, ScopedEventCatchup, StatusMessage, StrippedFInput,
    ZephyrCatchupManager,
};

use super::serverless;

pub mod events;
mod manager;
pub mod types;

pub struct GlobalCatchups<EI: EventInspector, ES: EventSource<EI>> {
    pub config: serverless::Config,
    pub catchup_receiver: mpsc::Receiver<ExecuteCatchupRequest>,
    pub event_source: Option<ES>,
    pub zephyr_manager: Option<ZephyrCatchupManager>,
    pub function_tx: mpsc::Sender<CatchupFunction>,
    _inspector: PhantomData<EI>,
}

impl<EI: EventInspector + Send + 'static, ES: EventSource<EI> + Send + Clone + 'static>
    GlobalCatchups<EI, ES>
{
    pub fn new(
        config: serverless::Config,
        event_source: Option<ES>,
        catchup_receiver: mpsc::Receiver<ExecuteCatchupRequest>,
        status_rx: mpsc::Receiver<StatusMessage>,
        db_sender: mpsc::Sender<DbInstructionWithCallback>,
    ) -> Self {
        let (ftx, frx) = mpsc::channel(20);

        let zephyr_manager = ZephyrCatchupManager::new(
            config.clone().executor_binary_path,
            frx,
            status_rx,
            db_sender,
        );
        Self {
            config,
            event_source,
            zephyr_manager: Some(zephyr_manager),
            _inspector: PhantomData {},
            function_tx: ftx,
            catchup_receiver,
        }
    }

    pub async fn consume(&mut self) {
        info!("starting global catchup worker");
        let (inbound_etx, mut inbound_erx) = mpsc::channel::<ExecuteCatchupRequest>(100);
        let (outbound_etx, mut outbound_erx) = mpsc::channel::<(Vec<u8>, EI)>(100);

        let es = self.event_source.clone();
        tokio::spawn(async move {
            debug!("main inner receiving task spawned");
            while let Some(new_request) = inbound_erx.recv().await {
                info!("got new catchup request");
                if let Some(es) = &es {
                    let ScopedEventCatchup {
                        contracts,
                        topic1s,
                        topic2s,
                        topic3s,
                        topic4s,
                        start,
                    } = new_request.scoped;
                    let maybe_events = es
                        .events_after_ledger_by_topics(
                            &contracts, &topic1s, &topic2s, &topic3s, &topic4s, start,
                        )
                        .await;

                    match maybe_events {
                        Ok(events) => {
                            info!("forwarding event inspector over to zephyr manager");
                            let _ = outbound_etx.send((new_request.hash, events)).await;
                        }
                        Err(e) => {
                            tracing::error!("unable to retrieve events from event source: {:?}", e);
                        }
                    }
                }
            }
        });

        let zephyr_manager = self.zephyr_manager.take();
        tokio::spawn(async move {
            debug!("started zephyr manager task");
            if let Some(mut zm) = zephyr_manager {
                zm.consume().await;
            }
        });

        loop {
            tokio::select! {
                new_catchup = self.catchup_receiver.recv() => {
                    if let Some(new_catchup) = new_catchup {
                        inbound_etx.send(new_catchup).await;
                    }
                }

                got_events = outbound_erx.recv() => {
                    if let Some((hash, got_events)) = got_events {
                        match self.build_and_send_catchup_functions(hash, got_events).await {
                            Ok(_) => tracing::info!("catchup sent successfully"),
                            Err(e) => tracing::error!("unable to send catchup {:?}", e),
                        }
                    }
                }
            }
        }
    }

    async fn build_and_send_catchup_functions(
        &self,
        hash: Vec<u8>,
        events_by_ledger: EI,
    ) -> anyhow::Result<()> {
        let mut first = None;
        for (ledger, (time, event_set)) in events_by_ledger.all_by_ledger().iter() {
            if first == None {
                first = Some(*ledger)
            }
            let meta = utils::ledger_meta_from_event_set(ledger, time, event_set);
            let xdr = meta.to_xdr(Limits::none())?;
            let finput = StrippedFInput::new(xdr, "on_close".into(), self.config.network_id());
            let catchup = CatchupFunction::new(hash.clone(), finput, first.unwrap_or(0) as u32);

            make_continue!(self.function_tx.send(catchup).await);
        }
        Ok(())
    }
}

mod utils {
    use crate::zephyr::sample::sample_ledger;
    use base64::prelude::*;
    use soroban_env_host::xdr::{
        AccountEntryExt, AccountEntryExtensionV1, AccountEntryExtensionV1Ext, ContractEvent,
        ContractEventV0, Hash, LedgerCloseMeta, LedgerEntry, LedgerEntryChanges, Limits,
        OperationMeta, ReadXdr, ScAddress, ScVal, SorobanTransactionMeta, TimePoint,
        TransactionMetaV3, TransactionResult, TransactionResultMeta, TransactionResultPair,
        TransactionResultResult, WriteXdr,
    };
    use std::convert::TryInto;

    use super::events::EventNode;

    fn to_txhash(v: Vec<u8>) -> [u8; 32] {
        v.try_into().unwrap_or_else(|v: Vec<u8>| {
            panic!("Expected a Vec of length {} but it was {}", 32, v.len())
        })
    }

    pub fn ledger_meta_from_event_set(
        ledger: &i64,
        time: &i64,
        event_set: &Vec<EventNode>,
    ) -> LedgerCloseMeta {
        let meta = LedgerCloseMeta::from_xdr_base64(sample_ledger(), Limits::none()).unwrap();
        let mut v1 = if let LedgerCloseMeta::V1(mut v1) = meta {
            v1.ledger_header.header.ledger_seq = *ledger as u32;
            v1.ledger_header.header.scp_value.close_time = TimePoint(*time as u64);
            v1
        } else {
            panic!()
        };

        let mut mut_tx_processing = v1.tx_processing.to_vec();

        for event in event_set {
            let event_tx_hash = {
                let vec = BASE64_STANDARD
                    .decode(&event.tx_info.tx_hash)
                    .unwrap_or([0; 32].to_vec());
                to_txhash(vec)
            };

            let result = TransactionResultMeta {
                result: TransactionResultPair {
                    transaction_hash: Hash(event_tx_hash),
                    result: TransactionResult {
                        fee_charged: 0,
                        result: TransactionResultResult::TxSuccess(vec![].try_into().unwrap()),
                        ext: soroban_env_host::xdr::TransactionResultExt::V0,
                    },
                },
                fee_processing: LedgerEntryChanges(vec![].try_into().unwrap()),
                tx_apply_processing: soroban_env_host::xdr::TransactionMeta::V3(
                    TransactionMetaV3 {
                        ext: soroban_env_host::xdr::ExtensionPoint::V0,
                        tx_changes_before: LedgerEntryChanges(vec![].try_into().unwrap()),
                        tx_changes_after: LedgerEntryChanges(vec![].try_into().unwrap()),
                        operations: vec![OperationMeta {
                            changes: LedgerEntryChanges(vec![].try_into().unwrap()),
                        }]
                        .try_into()
                        .unwrap(),
                        soroban_meta: Some(SorobanTransactionMeta {
                            ext: soroban_env_host::xdr::SorobanTransactionMetaExt::V0,
                            return_value: ScVal::Void,
                            diagnostic_events: vec![].try_into().unwrap(),
                            events: vec![ContractEvent {
                                ext: soroban_env_host::xdr::ExtensionPoint::V0,
                                contract_id: Some(Hash(
                                    stellar_strkey::Contract::from_string(&event.contract_id)
                                        .unwrap()
                                        .0,
                                )),
                                type_: soroban_env_host::xdr::ContractEventType::Contract,
                                body: soroban_env_host::xdr::ContractEventBody::V0(
                                    ContractEventV0 {
                                        topics: vec![
                                            ScVal::from_xdr_base64(
                                                event.topic1.clone().unwrap_or("".into()),
                                                Limits::none(),
                                            )
                                            .unwrap_or(ScVal::Void),
                                            ScVal::from_xdr_base64(
                                                event.topic2.clone().unwrap_or("".into()),
                                                Limits::none(),
                                            )
                                            .unwrap_or(ScVal::Void),
                                            ScVal::from_xdr_base64(
                                                event.topic3.clone().unwrap_or("".into()),
                                                Limits::none(),
                                            )
                                            .unwrap_or(ScVal::Void),
                                            ScVal::from_xdr_base64(
                                                event.topic4.clone().unwrap_or("".into()),
                                                Limits::none(),
                                            )
                                            .unwrap_or(ScVal::Void),
                                        ]
                                        .try_into()
                                        .unwrap(),
                                        data: ScVal::from_xdr_base64(
                                            event.data.clone(),
                                            Limits::none(),
                                        )
                                        .unwrap_or(ScVal::Void),
                                    },
                                ),
                            }]
                            .try_into()
                            .unwrap(),
                        }),
                    },
                ),
            };

            mut_tx_processing.push(result)
        }

        v1.tx_processing = mut_tx_processing.try_into().unwrap();
        LedgerCloseMeta::V1(v1)
    }
}
