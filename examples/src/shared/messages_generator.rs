use crate::shared::messages::{OrderConfirmed, OrderCreated, OrderRejected, SerializableMessage};
use iggy::utils::timestamp::IggyTimestamp;
use rand::rngs::ThreadRng;
use rand::Rng;

const CURRENCY_PAIRS: &[&str] = &["EUR/USD", "EUR/GBP", "USD/GBP", "EUR/PLN", "USD/PLN"];

#[derive(Debug, Default)]
pub struct MessagesGenerator {
    order_id: u64,
    rng: ThreadRng,
}

impl MessagesGenerator {
    pub fn new() -> MessagesGenerator {
        MessagesGenerator {
            order_id: 0,
            rng: rand::thread_rng(),
        }
    }

    pub fn generate(&mut self) -> Box<dyn SerializableMessage> {
        match self.rng.gen_range(0..=2) {
            0 => self.generate_order_created(),
            1 => self.generate_order_confirmed(),
            2 => self.generate_order_rejected(),
            _ => panic!("Unexpected message type"),
        }
    }

    fn generate_order_created(&mut self) -> Box<dyn SerializableMessage> {
        self.order_id += 1;
        Box::new(OrderCreated {
            order_id: self.order_id,
            timestamp: IggyTimestamp::now(),
            currency_pair: CURRENCY_PAIRS[self.rng.gen_range(0..CURRENCY_PAIRS.len())].to_string(),
            price: self.rng.gen_range(10.0..=1000.0),
            quantity: self.rng.gen_range(0.1..=1.0),
            side: match self.rng.gen_range(0..=1) {
                0 => "buy",
                _ => "sell",
            }
            .to_string(),
        })
    }

    fn generate_order_confirmed(&mut self) -> Box<dyn SerializableMessage> {
        Box::new(OrderConfirmed {
            order_id: self.order_id,
            timestamp: IggyTimestamp::now(),
            price: self.rng.gen_range(10.0..=1000.0),
        })
    }

    fn generate_order_rejected(&mut self) -> Box<dyn SerializableMessage> {
        Box::new(OrderRejected {
            order_id: self.order_id,
            timestamp: IggyTimestamp::now(),
            reason: match self.rng.gen_range(0..=1) {
                0 => "cancelled_by_user",
                _ => "other",
            }
            .to_string(),
        })
    }
}
