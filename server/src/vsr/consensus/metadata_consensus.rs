use crate::{
    utils::bitset::OptimalBitset,
    vsr::{
        client_table::ClientTable,
        header::{self, Prepare},
        message::{self, IggyMessage},
        status::Status,
        CommitNumber, OpNumber, Operation, QuorumCounter, ReplicaCount, ViewNumber,
    },
};
use std::{collections::HashMap, marker::PhantomData};

use self::message::LoopbackMessage;

pub struct MetadataConsensus<'msg, S, B> {
    id: u8,
    node_count: ReplicaCount,
    //TODO: Somehow tame this map, so it doesn't grow into infinity.
    prepare_acks: HashMap<OpNumber, OptimalBitset>,
    state_machine: S,
    message_bus: B,

    quorum_replication: ReplicaCount,
    quorum_view_change: ReplicaCount,
    quorum_nack_prepare: ReplicaCount,
    quroum_majority: ReplicaCount,

    // TODO: impl
    journal: PhantomData<()>,
    // TODO: not sure about this.. maybe it should live in the service call level.
    client_table: ClientTable,

    view_number: ViewNumber,
    log_view_number: ViewNumber,
    op_number: OpNumber,
    status: Status,
    commit_number: CommitNumber,

    loopback_queue: Option<LoopbackMessage<'msg>>,

    start_view_change_quorum: QuorumCounter<ViewNumber>,
    do_view_change_quorum: QuorumCounter<ViewNumber>,
}

impl<'msg, S, B> MetadataConsensus<'msg, S, B> {

    pub fn on_request(&mut self, request: message::Request) {
        assert!(self.is_primary());
        assert!(request.header.operation != Operation::Messages);
        assert!(request.header.view_number <= self.view_number);
        if self.status != Status::Normal {
            // Ignore
        }

        if self.view_number > request.header.view_number {
            // Ignore
        }

        self.enqueue_request(request);
    }

    fn enqueue_request(&mut self, request: message::Request) {
        assert!(self.status == Status::Normal);
        //TODO: Construct the header properly, instead of overwriting defaults.
        let mut header = header::Prepare::default();
        header.op_number = self.op_number + 1;
        header.commit_number = self.commit_number;
        let prepare = message::Prepare {
            header,
            command: request.command,
        };
        self.prepare_acks.insert(prepare.header.op_number, OptimalBitset::new(self.node_count as _));
        // Call `on_prepare`
        self.on_prepare(&prepare);
        assert!(self.op_number == prepare.header.op_number);
    }

    pub fn on_prepare(&self, prepare: &message::Prepare) {
        assert!(prepare.header.operation != Operation::Messages);
        assert!(prepare.header.view_number <= self.view_number);
        if self.status != Status::Normal {
            // Ignore
        }
        if self.view_number > prepare.header.view_number {
            // Ignore
        }
        assert!(prepare.header.replica == self.primary_idx(prepare.header.view_number));
        assert!(prepare.header.op_number > self.op_number);
        assert!(prepare.header.op_number > self.commit_number);

        // Message header.op <= self.op, what to do with this variant ?
        // This can happen due to replica being partitioned for certain `Prepare`
        // But then later one after the retry fires for that particular `Prepare`
        // It would eventually arrive.
        self.replicate(&prepare);
        if self.is_backup() {
            self.advance_commit_number(prepare.header.commit_number);
            assert!(self.commit_number >= prepare.header.commit_number);
        }
        // Commit to the state machine.
        // Put the prepare inside of the log.
        self.op_number = prepare.header.op_number;
        // Persist the prepare.
        self.append_prepare(&prepare);
        self.complete_loopback_queue();
    }

    fn advance_commit_number(&self, commit_number: u64) {
        //TODO: asserts.
        self.commit_number = commit_number;
    }

    fn replicate(&self, prepare: &message::Prepare) {
        //TODO: replicate it.
    }

    pub fn on_prepare_ok() {}

    fn append_prepare(&mut self, prepare: &message::Prepare) {
        //TODO: Append to the log.
        self.send_prepare_ok(&prepare);
    }

    pub fn complete_loopback_queue(&mut self) {
        let Some(message) = self.loopback_queue.take() else {
            return;
        };
        assert!(self.loopback_queue.is_none());
        //self.on_loopback_message(message);
        //TODO: call self.on_message(), once we get rid of the `ServerCommand`.
    }

    pub fn send_prepare_ok(&mut self, prepare: &message::Prepare) {
        //TODO: Create prepare_ok message using the prepare message header.
        let prepare_ok;
        let target_replica_id = self.primary_idx(self.view_number);
        self.send_message_to_replica(target_replica_id, prepare_ok);
    }

    pub fn send_message_to_replica(&mut self, target_replica_id: u8, message: &'msg IggyMessage) {
        if self.id == target_replica_id {
            let loopback_message = match message {
                IggyMessage::Prepare(prepare) => {
                    LoopbackMessage::Prepare(prepare)
                },
                IggyMessage::PrepareOk(prepare_ok) => {
                    LoopbackMessage::PrepareOk(prepare_ok)
                },
                _ => {
                    return;
                }
            };
            self.loopback_queue = Some(loopback_message);
            return;
        } else {
            //TODO: Send message to replica.
        }
    }

    fn primary_idx(&self, view_number: u32) -> u8 {
        let node_count = self.node_count;
        (view_number % node_count as u32) as u8
    }

    pub fn is_primary(&self) -> bool {
        self.primary_idx(self.view_number) == self.id
    }

    pub fn is_backup(&self) -> bool {
        !self.is_primary()
    }
}
