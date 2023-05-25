use crate::quic::command;
use crate::quic::quic_command::QuicCommand;
use crate::quic::sender::Sender;
use flume::Receiver;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::Mutex;
use tracing::error;

pub fn start(system: Arc<Mutex<System>>, receiver: Receiver<QuicCommand>) {
    tokio::spawn(async move {
        loop {
            let command = receiver.recv_async().await;
            if command.is_err() {
                error!("Error when receiving QUIC command: {:?}", command.err());
                continue;
            }

            let mut command = command.unwrap();
            let request = command.recv.read_to_end(1024 * 1024 * 1024).await;
            if request.is_err() {
                error!("Error when reading the QUIC request: {:?}", request);
                continue;
            }

            let mut system = system.lock().await;
            let result = command::handle(
                &request.unwrap(),
                &mut Sender { send: command.send },
                &mut system,
            )
            .await;
            if result.is_err() {
                error!("Error when handling the QUIC request: {:?}", result);
                continue;
            }
        }
    });
}
