use clap::ValueEnum;
use iggy::streams::get_streams::GetStreamsOutput;
use iggy::topics::get_topics::GetTopicsOutput;

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
