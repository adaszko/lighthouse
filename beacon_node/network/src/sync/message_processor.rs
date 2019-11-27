use super::manager::SyncMessage;
use crate::service::NetworkMessage;
use beacon_chain::{
    AttestationProcessingOutcome, BeaconChain, BeaconChainTypes, BlockProcessingOutcome,
};
use bls::SignatureSet;
use eth2_libp2p::rpc::methods::*;
use eth2_libp2p::rpc::{RPCEvent, RPCRequest, RPCResponse, RequestId};
use eth2_libp2p::PeerId;
use slog::{debug, error, info, o, trace, warn};
use ssz::Encode;
use state_processing::{
    common::get_indexed_attestation,
    per_block_processing::signature_sets::indexed_attestation_signature_set, per_slot_processing,
};
use std::sync::Arc;
use store::Store;
use tokio::sync::{mpsc, oneshot};
use tree_hash::SignedRoot;
use types::{Attestation, BeaconBlock, Domain, Epoch, EthSpec, Hash256, RelativeEpoch, Slot};

//TODO: Rate limit requests

/// If a block is more than `FUTURE_SLOT_TOLERANCE` slots ahead of our slot clock, we drop it.
/// Otherwise we queue it.
pub(crate) const FUTURE_SLOT_TOLERANCE: u64 = 1;

/// Keeps track of syncing information for known connected peers.
#[derive(Clone, Copy, Debug)]
pub struct PeerSyncInfo {
    fork_version: [u8; 4],
    pub finalized_root: Hash256,
    pub finalized_epoch: Epoch,
    pub head_root: Hash256,
    pub head_slot: Slot,
}

impl From<StatusMessage> for PeerSyncInfo {
    fn from(status: StatusMessage) -> PeerSyncInfo {
        PeerSyncInfo {
            fork_version: status.fork_version,
            finalized_root: status.finalized_root,
            finalized_epoch: status.finalized_epoch,
            head_root: status.head_root,
            head_slot: status.head_slot,
        }
    }
}

impl<T: BeaconChainTypes> From<&Arc<BeaconChain<T>>> for PeerSyncInfo {
    fn from(chain: &Arc<BeaconChain<T>>) -> PeerSyncInfo {
        Self::from(status_message(chain))
    }
}

/// Processes validated messages from the network. It relays necessary data to the syncing thread
/// and processes blocks from the pubsub network.
pub struct MessageProcessor<T: BeaconChainTypes> {
    /// A reference to the underlying beacon chain.
    chain: Arc<BeaconChain<T>>,
    /// A channel to the syncing thread.
    sync_send: mpsc::UnboundedSender<SyncMessage<T::EthSpec>>,
    /// A oneshot channel for destroying the sync thread.
    _sync_exit: oneshot::Sender<()>,
    /// A nextwork context to return and handle RPC requests.
    network: NetworkContext,
    /// The `RPCHandler` logger.
    log: slog::Logger,
}

impl<T: BeaconChainTypes> MessageProcessor<T> {
    /// Instantiate a `MessageProcessor` instance
    pub fn new(
        executor: &tokio::runtime::TaskExecutor,
        beacon_chain: Arc<BeaconChain<T>>,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
        log: &slog::Logger,
    ) -> Self {
        let sync_logger = log.new(o!("service"=> "sync"));
        let sync_network_context = NetworkContext::new(network_send.clone(), sync_logger.clone());

        // spawn the sync thread
        let (sync_send, _sync_exit) = super::manager::spawn(
            executor,
            Arc::downgrade(&beacon_chain),
            sync_network_context,
            sync_logger,
        );

        MessageProcessor {
            chain: beacon_chain,
            sync_send,
            _sync_exit,
            network: NetworkContext::new(network_send, log.clone()),
            log: log.clone(),
        }
    }

    fn send_to_sync(&mut self, message: SyncMessage<T::EthSpec>) {
        self.sync_send.try_send(message).unwrap_or_else(|_| {
            warn!(
                self.log,
                "Could not send message to the sync service";
            )
        });
    }

    /// Handle a peer disconnect.
    ///
    /// Removes the peer from the manager.
    pub fn on_disconnect(&mut self, peer_id: PeerId) {
        self.send_to_sync(SyncMessage::Disconnect(peer_id));
    }

    /// An error occurred during an RPC request. The state is maintained by the sync manager, so
    /// this function notifies the sync manager of the error.
    pub fn on_rpc_error(&mut self, peer_id: PeerId, request_id: RequestId) {
        self.send_to_sync(SyncMessage::RPCError(peer_id, request_id));
    }

    /// Handle the connection of a new peer.
    ///
    /// Sends a `Status` message to the peer.
    pub fn on_connect(&mut self, peer_id: PeerId) {
        self.network.send_rpc_request(
            None,
            peer_id,
            RPCRequest::Status(status_message(&self.chain)),
        );
    }

    /// Handle a `Status` request.
    ///
    /// Processes the `Status` from the remote peer and sends back our `Status`.
    pub fn on_status_request(
        &mut self,
        peer_id: PeerId,
        request_id: RequestId,
        status: StatusMessage,
    ) {
        // ignore status responses if we are shutting down
        trace!(self.log, "StatusRequest"; "peer" => format!("{:?}", peer_id));

        // Say status back.
        self.network.send_rpc_response(
            peer_id.clone(),
            request_id,
            RPCResponse::Status(status_message(&self.chain)),
        );

        self.process_status(peer_id, status);
    }

    /// Process a `Status` response from a peer.
    pub fn on_status_response(&mut self, peer_id: PeerId, status: StatusMessage) {
        trace!(self.log, "StatusResponse"; "peer" => format!("{:?}", peer_id));

        // Process the status message, without sending back another status.
        self.process_status(peer_id, status);
    }

    /// Process a `Status` message, requesting new blocks if appropriate.
    ///
    /// Disconnects the peer if required.
    fn process_status(&mut self, peer_id: PeerId, status: StatusMessage) {
        let remote = PeerSyncInfo::from(status);
        let local = PeerSyncInfo::from(&self.chain);

        let start_slot = |epoch: Epoch| epoch.start_slot(T::EthSpec::slots_per_epoch());

        if local.fork_version != remote.fork_version {
            // The node is on a different network/fork, disconnect them.
            debug!(
                self.log, "Handshake Failure";
                "peer" => format!("{:?}", peer_id),
                "reason" => "network_id"
            );

            self.network
                .disconnect(peer_id.clone(), GoodbyeReason::IrrelevantNetwork);
        } else if remote.head_slot
            > self.chain.slot().unwrap_or_else(|_| Slot::from(0u64)) + FUTURE_SLOT_TOLERANCE
        {
            // Note: If the slot_clock cannot be read, this will not error. Other system
            // components will deal with an invalid slot clock error.

            // The remotes head is on a slot that is significantly ahead of ours. This could be
            // because they are using a different genesis time, or that theirs or our system
            // clock is incorrect.
            debug!(
            self.log, "Handshake Failure";
            "peer" => format!("{:?}", peer_id),
            "reason" => "different system clocks or genesis time"
            );
            self.network
                .disconnect(peer_id.clone(), GoodbyeReason::IrrelevantNetwork);
        } else if remote.finalized_epoch <= local.finalized_epoch
            && remote.finalized_root != Hash256::zero()
            && local.finalized_root != Hash256::zero()
            && (self.chain.root_at_slot(start_slot(remote.finalized_epoch))
                != Some(remote.finalized_root))
        {
            // The remotes finalized epoch is less than or greater than ours, but the block root is
            // different to the one in our chain.
            //
            // Therefore, the node is on a different chain and we should not communicate with them.
            debug!(
                self.log, "Handshake Failure";
                "peer" => format!("{:?}", peer_id),
                "reason" => "different finalized chain"
            );
            self.network
                .disconnect(peer_id.clone(), GoodbyeReason::IrrelevantNetwork);
        } else if remote.finalized_epoch < local.finalized_epoch {
            // The node has a lower finalized epoch, their chain is not useful to us. There are two
            // cases where a node can have a lower finalized epoch:
            //
            // ## The node is on the same chain
            //
            // If a node is on the same chain but has a lower finalized epoch, their head must be
            // lower than ours. Therefore, we have nothing to request from them.
            //
            // ## The node is on a fork
            //
            // If a node is on a fork that has a lower finalized epoch, switching to that fork would
            // cause us to revert a finalized block. This is not permitted, therefore we have no
            // interest in their blocks.
            debug!(
                self.log,
                "NaivePeer";
                "peer" => format!("{:?}", peer_id),
                "reason" => "lower finalized epoch"
            );
        } else if self
            .chain
            .store
            .exists::<BeaconBlock<T::EthSpec>>(&remote.head_root)
            .unwrap_or_else(|_| false)
        {
            trace!(
                self.log, "Peer with known chain found";
                "peer" => format!("{:?}", peer_id),
                "remote_head_slot" => remote.head_slot,
                "remote_latest_finalized_epoch" => remote.finalized_epoch,
            );

            // If the node's best-block is already known to us and they are close to our current
            // head, treat them as a fully sync'd peer.
            self.send_to_sync(SyncMessage::AddPeer(peer_id, remote));
        } else {
            // The remote node has an equal or great finalized epoch and we don't know it's head.
            //
            // Therefore, there are some blocks between the local finalized epoch and the remote
            // head that are worth downloading.
            debug!(
                self.log, "UsefulPeer";
                "peer" => format!("{:?}", peer_id),
                "local_finalized_epoch" => local.finalized_epoch,
                "remote_latest_finalized_epoch" => remote.finalized_epoch,
            );
            self.send_to_sync(SyncMessage::AddPeer(peer_id, remote));
        }
    }

    /// Handle a `BlocksByRoot` request from the peer.
    pub fn on_blocks_by_root_request(
        &mut self,
        peer_id: PeerId,
        request_id: RequestId,
        request: BlocksByRootRequest,
    ) {
        let mut send_block_count = 0;
        for root in request.block_roots.iter() {
            if let Ok(Some(block)) = self.chain.store.get::<BeaconBlock<T::EthSpec>>(root) {
                self.network.send_rpc_response(
                    peer_id.clone(),
                    request_id,
                    RPCResponse::BlocksByRoot(block.as_ssz_bytes()),
                );
                send_block_count += 1;
            } else {
                debug!(
                    self.log,
                    "Peer requested unknown block";
                    "peer" => format!("{:?}", peer_id),
                    "request_root" => format!("{:}", root),
                );
            }
        }
        debug!(
            self.log,
            "Received BlocksByRoot Request";
            "peer" => format!("{:?}", peer_id),
            "requested" => request.block_roots.len(),
            "returned" => send_block_count,
        );

        // send stream termination
        self.network.send_rpc_error_response(
            peer_id,
            request_id,
            RPCErrorResponse::StreamTermination(ResponseTermination::BlocksByRoot),
        );
    }

    /// Handle a `BlocksByRange` request from the peer.
    pub fn on_blocks_by_range_request(
        &mut self,
        peer_id: PeerId,
        request_id: RequestId,
        req: BlocksByRangeRequest,
    ) {
        debug!(
            self.log,
            "Received BlocksByRange Request";
            "peer" => format!("{:?}", peer_id),
            "count" => req.count,
            "start_slot" => req.start_slot,
        );

        //TODO: Optimize this
        // Currently for skipped slots, the blocks returned could be less than the requested range.
        // In the current implementation we read from the db then filter out out-of-range blocks.
        // Improving the db schema to prevent this would be ideal.

        //TODO: This really needs to be read forward for infinite streams
        // We should be reading the first block from the db, sending, then reading the next... we
        // need a forwards iterator!!

        let mut blocks: Vec<BeaconBlock<T::EthSpec>> = self
            .chain
            .rev_iter_block_roots()
            .filter(|(_root, slot)| {
                req.start_slot <= slot.as_u64() && req.start_slot + req.count > slot.as_u64()
            })
            .take_while(|(_root, slot)| req.start_slot <= slot.as_u64())
            .filter_map(|(root, _slot)| {
                if let Ok(Some(block)) = self.chain.store.get::<BeaconBlock<T::EthSpec>>(&root) {
                    Some(block)
                } else {
                    warn!(
                        self.log,
                        "Block in the chain is not in the store";
                        "request_root" => format!("{:}", root),
                    );
                    None
                }
            })
            .filter(|block| block.slot >= req.start_slot)
            .collect();

        blocks.reverse();
        blocks.dedup_by_key(|brs| brs.slot);

        if blocks.len() < (req.count as usize) {
            debug!(
                self.log,
                "Sending BlocksByRange Response";
                "peer" => format!("{:?}", peer_id),
                "msg" => "Failed to return all requested blocks",
                "start_slot" => req.start_slot,
                "current_slot" => self.chain.slot().unwrap_or_else(|_| Slot::from(0_u64)).as_u64(),
                "requested" => req.count,
                "returned" => blocks.len(),
            );
        } else {
            trace!(
                self.log,
                "Sending BlocksByRange Response";
                "peer" => format!("{:?}", peer_id),
                "start_slot" => req.start_slot,
                "current_slot" => self.chain.slot().unwrap_or_else(|_| Slot::from(0_u64)).as_u64(),
                "requested" => req.count,
                "returned" => blocks.len(),
            );
        }

        for block in blocks {
            self.network.send_rpc_response(
                peer_id.clone(),
                request_id,
                RPCResponse::BlocksByRange(block.as_ssz_bytes()),
            );
        }
        // send the stream terminator
        self.network.send_rpc_error_response(
            peer_id,
            request_id,
            RPCErrorResponse::StreamTermination(ResponseTermination::BlocksByRange),
        );
    }

    /// Handle a `BlocksByRange` response from the peer.
    /// A `beacon_block` behaves as a stream which is terminated on a `None` response.
    pub fn on_blocks_by_range_response(
        &mut self,
        peer_id: PeerId,
        request_id: RequestId,
        beacon_block: Option<BeaconBlock<T::EthSpec>>,
    ) {
        trace!(
            self.log,
            "Received BlocksByRange Response";
            "peer" => format!("{:?}", peer_id),
        );

        self.send_to_sync(SyncMessage::BlocksByRangeResponse {
            peer_id,
            request_id,
            beacon_block,
        });
    }

    /// Handle a `BlocksByRoot` response from the peer.
    pub fn on_blocks_by_root_response(
        &mut self,
        peer_id: PeerId,
        request_id: RequestId,
        beacon_block: Option<BeaconBlock<T::EthSpec>>,
    ) {
        trace!(
            self.log,
            "Received BlocksByRoot Response";
            "peer" => format!("{:?}", peer_id),
        );

        self.send_to_sync(SyncMessage::BlocksByRootResponse {
            peer_id,
            request_id,
            beacon_block,
        });
    }

    /// Process a gossip message declaring a new block.
    ///
    /// Attempts to apply a block to the beacon chain. May queue the block for later processing.
    pub fn on_block_gossip(&mut self, peer_id: PeerId, block: BeaconBlock<T::EthSpec>) {
        match self.chain.process_block(block.clone()) {
            Ok(outcome) => match outcome {
                BlockProcessingOutcome::Processed { .. } => {
                    trace!(self.log, "Gossipsub block processed";
                            "peer_id" => format!("{:?}",peer_id));
                }
                BlockProcessingOutcome::ParentUnknown { parent: _ } => {
                    // Inform the sync manager to find parents for this block
                    trace!(self.log, "Block with unknown parent received";
                            "peer_id" => format!("{:?}",peer_id));
                    self.send_to_sync(SyncMessage::UnknownBlock(peer_id, block.clone()));
                }
                other => {
                    warn!(
                        self.log,
                        "Invalid gossip beacon block";
                        "outcome" => format!("{:?}", other),
                        "block root" => format!("{}", Hash256::from_slice(&block.signed_root()[..])),
                        "block slot" => block.slot
                    );
                    trace!(
                        self.log,
                        "Invalid gossip beacon block ssz";
                        "ssz" => format!("0x{}", hex::encode(block.as_ssz_bytes())),
                    );
                }
            },
            Err(e) => {
                error!(
                    self.log,
                    "Error processing gossip beacon block";
                    "error" => format!("{:?}", e),
                    "block slot" => block.slot
                );
                trace!(
                    self.log,
                    "Erroneous gossip beacon block ssz";
                    "ssz" => format!("0x{}", hex::encode(block.as_ssz_bytes())),
                );
            }
        }
    }

    /// Determines whether or not a given block is fit to be forwarded to other peers.
    pub fn should_forward_block(&mut self, block: BeaconBlock<T::EthSpec>) -> bool {
        // Retrieve the parent block used to generate the signature.
        // This will eventually return false if this operation fails or returns an empty option.
        let parent_block_opt = if let Ok(Some(parent_block)) =
            self.chain
                .store
                .get::<BeaconBlock<T::EthSpec>>(&block.parent_root)
        {
            // Check if the parent block's state root is equal to the current state, if it is, then
            // we can validate the block using the state in our chain head. This saves us from
            // having to make an unecessary database read.
            let state_res = if self.chain.head().beacon_state_root == parent_block.state_root {
                Ok(Some(self.chain.head().beacon_state.clone()))
            } else {
                self.chain
                    .store
                    .get_state(&parent_block.state_root, Some(block.slot))
            };

            // If we are unable to find a state for the block, we eventually return false. This
            // should never be the case though.
            match state_res {
                Ok(Some(state)) => Some((parent_block, state)),
                _ => None,
            }
        } else {
            None
        };

        // If we found a parent block and state to validate the signature with, we enter this
        // section and find the proposer for the block's slot, otherwise, we return false.
        if let Some((parent_block, mut state)) = parent_block_opt {
            // Determine if the block being validated is more than one epoch away from the parent.
            if block.slot.epoch(T::EthSpec::slots_per_epoch()) + 1
                > parent_block.slot.epoch(T::EthSpec::slots_per_epoch())
            {
                // If the block is more than one epoch in the future, we must fast-forward to the
                // state and compute the committee.
                for _ in state.slot.as_u64()..block.slot.as_u64() {
                    if per_slot_processing(&mut state, &self.chain.spec).is_err() {
                        // Return false if something goes wrong.
                        return false;
                    }
                }

                // Compute the committee cache so we can check the proposer.
                // TODO: Downvote peer
                if state
                    .build_committee_cache(RelativeEpoch::Current, &self.chain.spec)
                    .is_err()
                {
                    return false;
                }
            }

            // Compute the proposer for the block's slot.
            let proposer_result = state
                .get_beacon_proposer_index(block.slot, &self.chain.spec)
                .map(|i| state.validators.get(i));

            // Generate the domain that should have been used to create the signature.
            let domain = self.chain.spec.get_domain(
                block.slot.epoch(T::EthSpec::slots_per_epoch()),
                Domain::BeaconProposer,
                &state.fork,
            );

            // Verify the signature if we were able to get a proposer, otherwise, we eventually
            // return false.
            if let Ok(Some(proposer)) = proposer_result {
                let signature = SignatureSet::single(
                    &block.signature,
                    &proposer.pubkey,
                    block.signed_root(),
                    domain,
                );

                // TODO: Downvote if the signature is invalid.
                return signature.is_valid();
            }
        }

        false
    }

    /// Process a gossip message declaring a new attestation.
    ///
    /// Not currently implemented.
    pub fn on_attestation_gossip(&mut self, peer_id: PeerId, msg: Attestation<T::EthSpec>) {
        match self.chain.process_attestation(msg.clone()) {
            Ok(outcome) => match outcome {
                AttestationProcessingOutcome::Processed => {
                    info!(
                        self.log,
                        "Processed attestation";
                        "source" => "gossip",
                        "outcome" => format!("{:?}", outcome)
                    );
                }
                AttestationProcessingOutcome::UnknownHeadBlock { beacon_block_root } => {
                    // TODO: Maintain this attestation and re-process once sync completes
                    debug!(
                    self.log,
                    "Attestation for unknown block";
                    "peer_id" => format!("{:?}", peer_id),
                    "block" => format!("{}", beacon_block_root)
                    );
                    // we don't know the block, get the sync manager to handle the block lookup
                    self.send_to_sync(SyncMessage::UnknownBlockHash(peer_id, beacon_block_root));
                }
                AttestationProcessingOutcome::AttestsToFutureState { .. }
                | AttestationProcessingOutcome::FinalizedSlot { .. } => {} // ignore the attestation
                AttestationProcessingOutcome::Invalid { .. }
                | AttestationProcessingOutcome::EmptyAggregationBitfield { .. } => {
                    // the peer has sent a bad attestation. Remove them.
                    self.network.disconnect(peer_id, GoodbyeReason::Fault);
                }
            },
            Err(e) => {
                trace!(
                    self.log,
                    "Erroneous gossip attestation ssz";
                    "ssz" => format!("0x{}", hex::encode(msg.as_ssz_bytes())),
                );
                error!(self.log, "Invalid gossip attestation"; "error" => format!("{:?}", e));
            }
        };
    }

    /// Determines whether or not a given attestation is fit to be forwarded to other peers.
    pub fn should_forward_attestation(&self, attestation: Attestation<T::EthSpec>) -> bool {
        // Attempt to validate the attestation's signature against the head state.
        // In this case, we do not read anything from the database, which should be fast and will
        // work for most attestations that get passed around the network.
        let head_state = &self.chain.head().beacon_state;

        // Convert the attestation to an indexed attestation.
        if let Ok(indexed_attestation) = get_indexed_attestation(&head_state, &attestation) {
            // Validate the signature and return true if it is valid. Otherwise, we move on and read
            // the database to make certain we have the correct state.
            if let Ok(signature) = indexed_attestation_signature_set(
                &head_state,
                &indexed_attestation.signature,
                &indexed_attestation,
                &self.chain.spec,
            ) {
                // An invalid signature here does not necessarily mean the attestation is invalid.
                // It could be the case that our state has a different validator registry.
                if signature.is_valid() {
                    return true;
                }
            }
        }

        // If the first check did not pass, we retrieve the block for the beacon_block_root in the
        // attestation's data and use that to check the signature.
        if let Ok(Some(block)) = self
            .chain
            .store
            .get::<BeaconBlock<T::EthSpec>>(&attestation.data.beacon_block_root)
        {
            // Retrieve the block's state.
            if let Ok(Some(state)) = self
                .chain
                .store
                .get_state(&block.state_root, Some(block.slot))
            {
                // Convert the attestation to an indexed attestation.
                if let Ok(indexed_attestation) = get_indexed_attestation(&state, &attestation) {
                    // Check if the signature is valid against the state we got from the database.
                    if let Ok(signature) = indexed_attestation_signature_set(
                        &state,
                        &indexed_attestation.signature,
                        &indexed_attestation,
                        &self.chain.spec,
                    ) {
                        // TODO: Maybe downvote peer if the signature is invalid.
                        return signature.is_valid();
                    }
                }
            }
        }

        false
    }
}

/// Build a `StatusMessage` representing the state of the given `beacon_chain`.
pub(crate) fn status_message<T: BeaconChainTypes>(beacon_chain: &BeaconChain<T>) -> StatusMessage {
    let state = &beacon_chain.head().beacon_state;

    StatusMessage {
        fork_version: state.fork.current_version,
        finalized_root: state.finalized_checkpoint.root,
        finalized_epoch: state.finalized_checkpoint.epoch,
        head_root: beacon_chain.head().beacon_block_root,
        head_slot: state.slot,
    }
}

/// Wraps a Network Channel to employ various RPC/Sync related network functionality.
pub struct NetworkContext {
    /// The network channel to relay messages to the Network service.
    network_send: mpsc::UnboundedSender<NetworkMessage>,
    /// Logger for the `NetworkContext`.
    log: slog::Logger,
}

impl NetworkContext {
    pub fn new(network_send: mpsc::UnboundedSender<NetworkMessage>, log: slog::Logger) -> Self {
        Self { network_send, log }
    }

    pub fn disconnect(&mut self, peer_id: PeerId, reason: GoodbyeReason) {
        warn!(
            &self.log,
            "Disconnecting peer (RPC)";
            "reason" => format!("{:?}", reason),
            "peer_id" => format!("{:?}", peer_id),
        );
        self.send_rpc_request(None, peer_id.clone(), RPCRequest::Goodbye(reason));
        self.network_send
            .try_send(NetworkMessage::Disconnect { peer_id })
            .unwrap_or_else(|_| {
                warn!(
                    self.log,
                    "Could not send a Disconnect to the network service"
                )
            });
    }

    pub fn send_rpc_request(
        &mut self,
        request_id: Option<RequestId>,
        peer_id: PeerId,
        rpc_request: RPCRequest,
    ) {
        // use 0 as the default request id, when an ID is not required.
        let request_id = request_id.unwrap_or_else(|| 0);
        self.send_rpc_event(peer_id, RPCEvent::Request(request_id, rpc_request));
    }

    /// Convenience function to wrap successful RPC Responses.
    pub fn send_rpc_response(
        &mut self,
        peer_id: PeerId,
        request_id: RequestId,
        rpc_response: RPCResponse,
    ) {
        self.send_rpc_event(
            peer_id,
            RPCEvent::Response(request_id, RPCErrorResponse::Success(rpc_response)),
        );
    }

    /// Send an RPCErrorResponse. This handles errors and stream terminations.
    pub fn send_rpc_error_response(
        &mut self,
        peer_id: PeerId,
        request_id: RequestId,
        rpc_error_response: RPCErrorResponse,
    ) {
        self.send_rpc_event(peer_id, RPCEvent::Response(request_id, rpc_error_response));
    }

    fn send_rpc_event(&mut self, peer_id: PeerId, rpc_event: RPCEvent) {
        self.network_send
            .try_send(NetworkMessage::RPC(peer_id, rpc_event))
            .unwrap_or_else(|_| {
                warn!(
                    self.log,
                    "Could not send RPC message to the network service"
                )
            });
    }
}
