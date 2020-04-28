use multiqueue2 as multiqueue;
use serde_derive::{Deserialize, Serialize};
use slog::{error, Logger};
use std::marker::PhantomData;
use std::sync::Mutex;
use types::{Attestation, Epoch, EthSpec, Hash256, SignedBeaconBlock, SignedBeaconBlockHash};
pub use websocket_server::WebSocketSender;

pub trait EventHandler<T: EthSpec>: Sized + Send + Sync {
    fn register(&self, kind: EventKind<T>) -> Result<(), String>;
}

pub struct NullEventHandler<T: EthSpec>(PhantomData<T>);

impl<T: EthSpec> EventHandler<T> for WebSocketSender<T> {
    fn register(&self, kind: EventKind<T>) -> Result<(), String> {
        self.send_string(
            serde_json::to_string(&kind)
                .map_err(|e| format!("Unable to serialize event: {:?}", e))?,
        )
    }
}

pub struct ServerSentEvents<T: EthSpec> {
    head_changed_queue_sender: Mutex<multiqueue::MPMCFutSender<SignedBeaconBlockHash>>,
    log: Logger,
    _phantom: PhantomData<T>,
}

impl<T: EthSpec> ServerSentEvents<T> {
    pub fn new(log: Logger) -> (Self, multiqueue::MPMCFutReceiver<SignedBeaconBlockHash>) {
        let (sender, receiver) = multiqueue::mpmc_fut_queue(T::slots_per_epoch());
        let this = Self {
            head_changed_queue_sender: Mutex::new(sender),
            log: log,
            _phantom: PhantomData,
        };
        (this, receiver)
    }
}

impl<T: EthSpec> EventHandler<T> for ServerSentEvents<T> {
    fn register(&self, kind: EventKind<T>) -> Result<(), String> {
        match kind {
            EventKind::BeaconHeadChanged {
                current_head_beacon_block_root,
                ..
            } => {
                let guard = self
                    .head_changed_queue_sender
                    .lock()
                    .map_err(|_| "Cannot lock mutex")?;
                if let Err(_) = guard.try_send(current_head_beacon_block_root.into()) {
                    error!(
                        self.log,
                        "Head change streaming queue full; dropping change: {}",
                        current_head_beacon_block_root
                    );
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl<T: EthSpec> EventHandler<T> for NullEventHandler<T> {
    fn register(&self, _kind: EventKind<T>) -> Result<(), String> {
        Ok(())
    }
}

impl<T: EthSpec> Default for NullEventHandler<T> {
    fn default() -> Self {
        NullEventHandler(PhantomData)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(
    bound = "T: EthSpec",
    rename_all = "snake_case",
    tag = "event",
    content = "data"
)]
pub enum EventKind<T: EthSpec> {
    BeaconHeadChanged {
        reorg: bool,
        current_head_beacon_block_root: Hash256,
        previous_head_beacon_block_root: Hash256,
    },
    BeaconFinalization {
        epoch: Epoch,
        root: Hash256,
    },
    BeaconBlockImported {
        block_root: Hash256,
        block: Box<SignedBeaconBlock<T>>,
    },
    BeaconBlockRejected {
        reason: String,
        block: Box<SignedBeaconBlock<T>>,
    },
    BeaconAttestationImported {
        attestation: Box<Attestation<T>>,
    },
    BeaconAttestationRejected {
        reason: String,
        attestation: Box<Attestation<T>>,
    },
}
