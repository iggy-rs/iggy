use clap::{ArgGroup, Args, Subcommand};
use iggy::identifier::Identifier;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum MessageAction {
    /// Send messages to given topic ID and given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples
    ///  iggy message send 1 2 message
    ///  iggy message send stream 2 "long message"
    ///  iggy message send 1 topic message1 message2 message3
    ///  iggy message send stream topic "long message with spaces"
    #[clap(verbatim_doc_comment, visible_alias = "s")]
    Send(SendMessagesArgs),
    /// Poll messages from given topic ID and given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples:
    ///  iggy message poll --offset 0 1 2 1
    ///  iggy message poll --offset 0 stream 2 1
    ///  iggy message poll --offset 0 1 topic 1
    ///  iggy message poll --offset 0 stream topic 1
    #[clap(verbatim_doc_comment, visible_alias = "p")]
    Poll(PollMessagesArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct SendMessagesArgs {
    /// ID of the stream to which the message will be sent
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// ID of the topic to which the message will be sent
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// ID of the partition to which the message will be sent
    #[clap(short, long, group = "partitioning")]
    pub(crate) partition_id: Option<u32>,
    /// Messages key which will be used to partition the messages
    ///
    /// Value of the key will be used by the server to calculate the partition ID
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, group = "partitioning")]
    pub(crate) message_key: Option<String>,
    /// Messages to be sent
    ///
    /// If no messages are provided, the command will read the messages from the
    /// standard input and each line will be sent as a separate message.
    /// If messages are provided, they will be sent as is. If message contains
    /// spaces, it should be enclosed in quotes. Limit of the messages and size
    /// of each message is defined by the used shell.
    #[clap(verbatim_doc_comment)]
    pub(crate) messages: Option<Vec<String>>,
}

#[derive(Debug, Clone, Args)]
#[command(group = ArgGroup::new("polling_strategy").required(true))]
pub(crate) struct PollMessagesArgs {
    /// ID of the stream from which message will be polled
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// ID of the topic from which message will be polled
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// Partition ID from which message will be polled
    #[arg(value_parser = clap::value_parser!(u32).range(1..))]
    pub(crate) partition_id: u32,
    /// Number of messages to poll
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    pub(crate) message_count: u32,
    /// Auto commit offset
    ///
    /// Flag indicates whether to commit offset on the server automatically
    /// after polling the messages.
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = false)]
    pub(crate) auto_commit: bool,
    /// Polling strategy - offset to start polling messages from
    ///
    /// Offset must be specified as a number
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, group = "polling_strategy")]
    pub(crate) offset: Option<u64>,
    /// Polling strategy - start polling from the first message in the partition
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = false, group = "polling_strategy")]
    pub(crate) first: bool,
    /// Polling strategy - start polling from the last message in the partition
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = false, group = "polling_strategy")]
    pub(crate) last: bool,
    /// Polling strategy - start polling from the next message
    ///
    /// Start polling after the last polled message based
    /// on the stored consumer offset
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = false, group = "polling_strategy")]
    pub(crate) next: bool,
    /// Regular consumer which will poll messages
    ///
    /// Consumer ID can be specified as a consumer name or ID
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = Identifier::default(), value_parser = clap::value_parser!(Identifier))]
    pub(crate) consumer: Identifier,
}
