use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub const ORDER_CREATED_TYPE: &str = "order_created";
pub const ORDER_CONFIRMED_TYPE: &str = "order_confirmed";
pub const ORDER_REJECTED_TYPE: &str = "order_rejected";

pub trait SerializableMessage: Debug {
    fn to_json_envelope(&self) -> String;
}

// The message envelope can be used to send the different types of messages to the same topic.
#[derive(Debug, Deserialize, Serialize)]
pub struct Envelope {
    pub message_type: String,
    pub payload: String,
}

impl Envelope {
    pub fn new<T>(message_type: &str, payload: &T) -> Envelope
    where
        T: Serialize,
    {
        Envelope {
            message_type: message_type.to_string(),
            payload: serde_json::to_string(payload).unwrap(),
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderCreated {
    pub id: u64,
    pub currency_pair: String,
    pub price: f64,
    pub quantity: f64,
    pub side: String,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderConfirmed {
    pub id: u64,
    pub price: f64,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderRejected {
    pub id: u64,
    pub timestamp: u64,
    pub reason: String,
}

impl SerializableMessage for OrderCreated {
    fn to_json_envelope(&self) -> String {
        Envelope::new(ORDER_CREATED_TYPE, self).to_json()
    }
}

impl SerializableMessage for OrderConfirmed {
    fn to_json_envelope(&self) -> String {
        Envelope::new(ORDER_CONFIRMED_TYPE, self).to_json()
    }
}

impl SerializableMessage for OrderRejected {
    fn to_json_envelope(&self) -> String {
        Envelope::new(ORDER_REJECTED_TYPE, self).to_json()
    }
}
