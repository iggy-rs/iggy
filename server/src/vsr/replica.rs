use super::*;
use crate::versioning::SemanticVersion;
use client_table::ClientTable;
use status::Status;
use std::marker::PhantomData;

// TODO: trait bounds
pub struct Replica<S, B> {
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
    // This is where we differ from the VRR paper, we use `commit_min` and `commit_max`
    // to slice the log during the state transfer protocol.
    commit_min: CommitNumber,
    commit_max: CommitNumber,

    // TODO: sometimes replica sends message to itself (e.g acking our own prepare or when starting view change).
    loopback_queue: PhantomData<()>,

    start_view_change_quorum: QuorumCounter<ViewNumber>,
    do_view_change_quorum: QuorumCounter<ViewNumber>,
    // TODO: section with auxiliary data, related to messages (user data).
}
