#[derive(Debug)]
pub enum ServerCommand {
    HandleRequest((quinn::SendStream, quinn::RecvStream)),
    SaveMessages,
    Shutdown,
}
