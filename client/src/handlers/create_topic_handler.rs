use crate::handlers::response_handler::handle_status;
use std::io;
use std::str::from_utf8;
use tokio::net::UdpSocket;

const COMMAND: &[u8] = &[11];
const PARTS: usize = 3;

pub async fn handle(input: &[&str], socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    if input.len() != PARTS {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Invalid create topic command, expected {} parts.", PARTS),
        ));
    }

    let id = input[0].parse::<u32>();
    if let Err(error) = id {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let partitions_count = input[1].parse::<u32>();
    if let Err(error) = partitions_count {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let partitions_count = partitions_count.unwrap();
    if !(1..=100).contains(&partitions_count) {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Invalid number of partitions: {}, expected between 1 and 100.",
                partitions_count
            ),
        ));
    }

    let name = from_utf8(input[2].as_bytes()).unwrap();
    if name.len() > 100 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Invalid topic name: {}, expected between 1 and 100 characters.",
                name
            ),
        ));
    }

    let id = &id.unwrap().to_le_bytes();
    let partitions_count = &partitions_count.to_le_bytes();
    let name = name.as_bytes();

    socket
        .send([COMMAND, id, partitions_count, name].concat().as_slice())
        .await?;
    handle_response(socket, buffer).await?;
    Ok(())
}

async fn handle_response(socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    socket.recv(buffer).await?;
    handle_status(buffer)?;
    Ok(())
}
