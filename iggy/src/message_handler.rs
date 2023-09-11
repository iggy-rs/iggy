use crate::models::messages::Message;
use std::fmt::Debug;

pub trait MessageHandler: Send + Sync + Debug {
    fn handle(&self, message: Message);
}
