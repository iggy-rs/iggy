use crate::cli::CliCommand;

use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::streams::delete_stream::DeleteStream;

#[derive(Debug)]
pub(crate) struct StreamDelete {
    id: u32,
}

impl StreamDelete {
    pub(crate) fn new(id: u32) -> Self {
        Self { id }
    }
}

#[async_trait]
impl CliCommand for StreamDelete {
    fn explain(&self) -> String {
        format!("delete stream {}", self.id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) {
        match client
            .delete_stream(&DeleteStream {
                stream_id: Identifier::numeric(self.id).expect("Expected numeric identifier"),
            })
            .await
        {
            Ok(_) => {
                println!("Stream with id: {} deleted", self.id);
            }
            Err(err) => {
                println!("Problem creating stream (id: {}): {err}", self.id);
            }
        }
    }
}
