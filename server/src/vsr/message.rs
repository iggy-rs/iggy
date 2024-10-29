use crate::command::ServerCommand;
use bytes::Bytes;

use super::header;

pub enum IggyMessage {
    Request(Request),
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    //TODO: Add Request variant.
}

pub enum LoopbackMessage<'msg> {
    Prepare(&'msg Prepare),
    PrepareOk(&'msg PrepareOk),
}

pub struct Request {
    pub header: header::Request,
    pub command: ServerCommand,
}

pub struct Prepare {
    pub header: header::Prepare,
    pub command: ServerCommand,
}

pub struct PrepareOk {
    pub header: header::PrepareOk,
    pub body: Bytes,
}

impl IggyMessage {
    pub fn is_prepare(&self) -> bool {
        matches!(self, IggyMessage::Prepare(_))
    }

    pub fn is_prepare_ok(&self) -> bool {
        matches!(self, IggyMessage::PrepareOk(_))
    }
}
