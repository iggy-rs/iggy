use super::*;
use crate::{command::ServerCommand, versioning::SemanticVersion};
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
    commit_max: CommitNumber,

    // TODO: sometimes replica sends message to itself (e.g acking our own prepare or when starting view change).
    loopback_queue: PhantomData<()>,

    start_view_change_quorum: QuorumCounter<ViewNumber>,
    do_view_change_quorum: QuorumCounter<ViewNumber>,
    // TODO: section with auxiliary data, related to messages (user data).
}

impl<S,B> Replica<S,B> {
    fn on_command(&self, command: ServerCommand) {
        match command {
            ServerCommand::CreateStream(command) => {

            },
            ServerCommand::UpdateStream(command) => {

            }
            ServerCommand::DeleteStream(command) => {

            }
            ServerCommand::PurgeStream(command) => {

            }
            ServerCommand::CreateTopic(command) => {

            }
            ServerCommand::UpdateTopic(command) => {

            }
            ServerCommand::DeleteTopic(command) => {

            }
            ServerCommand::PurgeTopic(command) => {

            }
            ServerCommand::CreatePartitions(command) => {

            }
            ServerCommand::DeletePartitions(command) => {

            }
            ServerCommand::CreateConsumerGroup(command) => {

            }
            ServerCommand::DeleteConsumerGroup(command) => {

            }
            ServerCommand::CreateUser(command) => {

            }
            ServerCommand::UpdateUser(command) => {

            }
            ServerCommand::DeleteUser(command) => {

            }
            ServerCommand::ChangePassword(command) => {

            }
            ServerCommand::UpdatePermissions(command) => {

            }
            ServerCommand::CreatePersonalAccessToken(command) => {

            }
            ServerCommand::DeletePersonalAccessToken(command) => {

            }
            _ => { unreachable!();}
        }
    }
}
