use crate::args::ListMode;
use crate::cli::CliCommand;

use async_trait::async_trait;
use comfy_table::Table;
use iggy::client::Client;
use iggy::streams::get_streams::GetStreams;
use iggy::utils::timestamp::TimeStamp;

#[derive(Debug)]
pub(crate) struct StreamList {
    mode: ListMode,
}

impl StreamList {
    pub(crate) fn new(mode: ListMode) -> Self {
        Self { mode }
    }
}

#[async_trait]
impl CliCommand for StreamList {
    fn explain(&self) -> String {
        let mode = match self.mode {
            ListMode::Table => "table",
            ListMode::List => "list",
        };
        format!("list streams in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) {
        match client.get_streams(&GetStreams {}).await {
            Ok(streams) => {
                if streams.is_empty() {
                    println!("No streams found!");
                } else {
                    match self.mode {
                        ListMode::Table => {
                            let mut table = Table::new();

                            table.set_header(vec![
                                "ID", "Created", "Name", "Size", "Messages", "Topics",
                            ]);

                            streams.iter().for_each(|stream| {
                                table.add_row(vec![
                                    format!("{}", stream.id),
                                    TimeStamp::from(stream.created_at)
                                        .to_string("%Y-%m-%d %H:%M:%S"),
                                    stream.name.clone(),
                                    format!("{}", stream.size_bytes),
                                    format!("{}", stream.messages_count),
                                    format!("{}", stream.topics_count),
                                ]);
                            });

                            println!("{table}");
                        }
                        ListMode::List => {
                            streams.iter().for_each(|stream| {
                                println!(
                                    "{}|{}|{}|{}|{}|{}",
                                    stream.id,
                                    TimeStamp::from(stream.created_at)
                                        .to_string("%Y-%m-%d %H:%M:%S"),
                                    stream.name,
                                    stream.size_bytes,
                                    stream.messages_count,
                                    stream.topics_count
                                );
                            });
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("Problem getting streams {err}");
            }
        }
    }
}
