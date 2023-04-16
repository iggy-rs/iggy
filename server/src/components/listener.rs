use crate::server::Server;
use crate::server_command::ServerCommand;
use bytes::BytesMut;
use std::io;
use tracing::info;

impl Server {
    pub async fn start_listener(&self) -> Result<(), io::Error> {
        loop {
            let mut buffer = BytesMut::zeroed(1024);
            let (length, address) = self.socket.recv_from(&mut buffer).await?;
            buffer.truncate(length);
            info!("{:?} bytes received from {:?}", length, address);
            self.sender
                .send(ServerCommand::HandleRequest(buffer.freeze(), address))
                .await
                .unwrap();
        }
    }
}
