use std::marker::PhantomData;
use events::{EventInspector, EventSource};
use tokio::sync::mpsc;
use types::{CatchupFunction, CatchupRequest, StatusMessage, ZephyrCatchupManager};

mod manager;
mod types;
mod events;

pub struct GlobalCatchups<EI: EventInspector, ES: EventSource<EI>> {
    pub catchup_receiver: mpsc::Receiver<CatchupRequest>,
    pub event_source: Option<ES>,
    pub zephyr_manager: ZephyrCatchupManager,
    pub function_tx: mpsc::Sender<CatchupFunction>,
    _inspector: PhantomData<EI>
}

impl<EI: EventInspector, ES: EventSource<EI>> GlobalCatchups<EI, ES> {
    pub fn new(binary_path: String, event_source: Option<ES>, catchup_receiver: mpsc::Receiver<CatchupRequest>, status_tx: mpsc::Receiver<StatusMessage>) -> Self {
        let (ftx, frx) = mpsc::channel(20);
        
        let zephyr_manager = ZephyrCatchupManager::new(binary_path, frx, status_tx);
        Self { event_source, zephyr_manager, _inspector: PhantomData {}, function_tx: ftx, catchup_receiver}
    }

    pub async fn consume(&mut self) {
        while let Some(new_catchup) = self.catchup_receiver.recv().await {
            
        }
    }
}
