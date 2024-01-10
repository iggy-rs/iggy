use clap::ValueEnum;
use iggy::cmd::client::get_clients::GetClientsOutput;
use iggy::cmd::consumer_group::get_consumer_groups::GetConsumerGroupsOutput;
use iggy::cmd::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokensOutput;
use iggy::cmd::streams::get_streams::GetStreamsOutput;
use iggy::cmd::topics::get_topics::GetTopicsOutput;
use iggy::cmd::users::get_users::GetUsersOutput;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum ListMode {
    Table,
    List,
}

impl From<ListMode> for GetStreamsOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetStreamsOutput::Table,
            ListMode::List => GetStreamsOutput::List,
        }
    }
}

impl From<ListMode> for GetTopicsOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetTopicsOutput::Table,
            ListMode::List => GetTopicsOutput::List,
        }
    }
}

impl From<ListMode> for GetPersonalAccessTokensOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetPersonalAccessTokensOutput::Table,
            ListMode::List => GetPersonalAccessTokensOutput::List,
        }
    }
}

impl From<ListMode> for GetUsersOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetUsersOutput::Table,
            ListMode::List => GetUsersOutput::List,
        }
    }
}

impl From<ListMode> for GetClientsOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetClientsOutput::Table,
            ListMode::List => GetClientsOutput::List,
        }
    }
}

impl From<ListMode> for GetConsumerGroupsOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetConsumerGroupsOutput::Table,
            ListMode::List => GetConsumerGroupsOutput::List,
        }
    }
}
