use clap::ValueEnum;
use iggy::cmd::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokensOutput;
use iggy::cmd::streams::get_streams::GetStreamsOutput;
use iggy::cmd::topics::get_topics::GetTopicsOutput;

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
