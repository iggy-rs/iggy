use crate::binary::BinaryTransport;
use crate::client::Client;
use crate::client_v2::ClientV2;
use async_trait::async_trait;

/// A client that can send and receive binary messages.
#[async_trait]
pub trait BinaryClient: BinaryTransport + Client {}

/// A client that can send and receive binary messages.
#[async_trait]
pub trait BinaryClientV2: BinaryTransport + ClientV2 {}
