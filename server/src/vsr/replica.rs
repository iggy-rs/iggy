use super::*;
use crate::{command::ServerCommand, versioning::SemanticVersion};
use client_table::ClientTable;
use consensus::metadata_consensus::MetadataConsensus;
use header::{Header, Prepare};
use iggy::bytes_serializable::BytesSerializable;
use message::IggyMessage;
use status::Status;
use std::marker::PhantomData;

// TODO: trait bounds
// TODO: seperate a `consesus` instance that we will have there.
pub struct Replica<'msg, S, B> {
    id: usize,
    cluster: u128,
    node_count: usize,

    quorum_replication: usize,
    quorum_view_change: usize,
    quorum_nack_prepare: usize,
    quroum_majority: usize,

    version: SemanticVersion,

    // TODO: maybe
    min_version: SemanticVersion,
    // TODO: maybe support hot swapping of versions.


    //TODO: Replace with concrete `StateMachine` impl instead of forwarding a generic.
    metadata_consensus: MetadataConsensus<S>,
    // TODO: impl
    journal: PhantomData<()>,
    // TODO: not sure about this.. maybe it should live in the service call level.
    client_sessions: PhantomData<()>,
    client_table: ClientTable,

    state_machine: S,
    message_bus: B,

    view_number: ViewNumber,
    log_view_number: ViewNumber,
    op_number: OpNumber,
    status: Status,
    commit_max: CommitNumber,

    loopback_queue: Option<&'msg IggyMessage>,

    start_view_change_quorum: QuorumCounter<ViewNumber>,
    do_view_change_quorum: QuorumCounter<ViewNumber>,
    // TODO: section with auxiliary data, related to messages (user data).
}
impl<S, B> Replica<'_, S, B> {
    // TODO: Implement lol.
    pub fn new() {}
}

impl<'msg, S, B> Replica<'msg, S, B> {
    // TODO: Switch to accepting a `IggyMessage` struct, once the `Request` variant is implemented.
    fn on_message(&self, command: ServerCommand) {
        let body = command.to_bytes();
        //TODO: Create proper prepare header.
        let prepare = Prepare::default();
        let header = Header::Prepare(prepare);
        let message = IggyMessage::new(header, body);

        //TODO: Create a pipeline of requests/prepares..
        self.on_prepare(message);
    }

    fn is_primary(&self) -> bool {
        //TODO: Impl.
        true
    }

    fn is_backup(&self) -> bool {
        //TODO: Impl.
        false
    }

    fn replicate(&self, message: IggyMessage) {
        assert!(message.header.is_prepare());
        //TODO: replicate it.
    }

    fn on_prepare(&self, message: IggyMessage) {
        assert!(message.header.is_prepare());

        self.replicate(message);
        // TODO: Advance commit_number.
        // Message header.op > self.op + 1, what to do with this invariant ?.
        self.send_prepare_ok(&message);

    }

    fn on_prepare_ok() {
    }

    fn complete_loopback_queue(&mut self) {
        let Some(message) = self.loopback_queue else {
            return;
        };
        //TODO: call self.on_message(), once we get rid of the `ServerCommand`.
    }

    fn send_prepare_ok(&mut self, message: &'msg IggyMessage) {
        assert!(message.header.is_prepare_ok());
        //TODO: Create prepare_ok message using the prepare message header.
        self.send_message_to_replica(message);
        self.complete_loopback_queue();
    }

    fn send_header_to_replica(&self, header: Header) {

    }

    fn send_message_to_replica(&mut self, message: &'msg IggyMessage) {
        if self.is_primary() {
            self.loopback_queue = Some(message);
            return;
        }
    }
}
