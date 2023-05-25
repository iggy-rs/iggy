use quinn::{RecvStream, SendStream};

#[derive(Debug)]
pub struct QuicCommand {
    pub send: SendStream,
    pub recv: RecvStream,
}
