use super::*;
use crate::{command::ServerCommand, versioning::SemanticVersion};
use consensus::metadata_consensus::MetadataConsensus;
use header::Prepare;
use iggy::bytes_serializable::BytesSerializable;
use message::IggyMessage;
use std::marker::PhantomData;

// TODO: trait bounds
// TODO: seperate a `consesus` instance that we will have there.
pub struct Replica<'msg, S, B> {
    id: u8,
    cluster: u128,
    node_count: ReplicaCount,

    version: SemanticVersion,
    // TODO: maybe
    min_version: SemanticVersion,
    // TODO: maybe support hot swapping of versions.

    //TODO: Replace with concrete `StateMachine` and `MessageBus` impl instead of forwarding a generic.
    metadata_consensus: MetadataConsensus<'msg, S, B>,
    // TODO: not sure about this.. maybe it should live in the service call level.
    client_sessions: PhantomData<()>,
}
impl<S, B> Replica<'_, S, B> {
    // TODO: Implement lol.
    pub fn new() {}
}

impl<'msg, S, B> Replica<'msg, S, B> {
    fn top_level_call(&self, command: ServerCommand) {
        //TODO: handle other cases.
        //TODO: Create proper prepare header.
        let header = header::Prepare::default();
        let prepare = message::Prepare { header, command };
        let message = IggyMessage::Prepare(prepare);
        self.on_message(message);
    }

    // TODO: Switch to accepting a `IggyMessage` struct, once the `Request` variant is implemented.
    fn on_message(&self, message: IggyMessage) {
        match message {
            IggyMessage::Request(request) => self.on_request(request),
            IggyMessage::Prepare(prepare) => self.on_prepare(prepare),
            IggyMessage::PrepareOk(prepare_ok) => todo!(),
        }
    }

    fn on_request(&self, request: message::Request) {
        assert!(request.header.replica < self.node_count);
        if request.header.version > self.version {
            // Ignore
        }
        self.metadata_consensus.on_request(request);
    }

    fn on_prepare(&self, prepare: message::Prepare) {
        //TODO: Create a container of requests/prepares..
        assert!(prepare.header.replica < self.node_count);
        if prepare.header.version > self.version {
            // Ignore
        }
        self.metadata_consensus.on_prepare(&prepare);
    }

    fn on_prepare_ok() {}
}
