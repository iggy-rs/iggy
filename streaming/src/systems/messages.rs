use crate::message::Message;
use crate::polling_consumer::PollingConsumer;
use crate::systems::system::System;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm};
use bytes::Bytes;
use iggy::error::Error;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::messages::send_messages;
use iggy::messages::send_messages::Partitioning;
use std::sync::Arc;
use tracing::{error, trace};

impl System {
    pub async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer: PollingConsumer,
        partition_id: u32,
        strategy: PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<Vec<Arc<Message>>, Error> {
        if count == 0 {
            return Err(Error::InvalidMessagesCount);
        }

        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        if !topic.has_partitions() {
            return Err(Error::NoPartitions(topic.id, topic.stream_id));
        }

        let partition_id = match consumer {
            PollingConsumer::Consumer(_) => partition_id,
            PollingConsumer::ConsumerGroup(consumer_group_id, member_id) => {
                let consumer_group = topic.get_consumer_group(consumer_group_id)?.read().await;
                consumer_group.calculate_partition_id(member_id).await?
            }
        };

        let messages = topic
            .get_messages(consumer, partition_id, strategy, count)
            .await?;

        if messages.is_empty() {
            return Ok(messages);
        }

        let offset = messages.last().unwrap().offset;
        if auto_commit {
            trace!("Last offset: {} will be automatically stored for {}, stream: {}, topic: {}, partition: {}", offset, consumer, stream_id, topic_id, partition_id);
            topic
                .store_consumer_offset(consumer, partition_id, offset)
                .await?;
        }

        if self.cipher.is_none() {
            return Ok(messages);
        }

        let cipher = self.cipher.as_ref().unwrap();
        let mut decrypted_messages = Vec::with_capacity(messages.len());
        for message in messages {
            let nonce = GenericArray::from_slice(&message.payload[0..12]);
            let payload = cipher.decrypt(nonce, &message.payload[12..]);
            if payload.is_err() {
                error!("Cannot decrypt the message.");
                return Err(Error::CannotDecryptMessage);
            }

            let payload = payload.unwrap();
            decrypted_messages.push(Arc::new(Message {
                id: message.id,
                offset: message.offset,
                timestamp: message.timestamp,
                checksum: message.checksum,
                length: payload.len() as u32,
                payload: Bytes::from(payload),
            }));
        }

        Ok(decrypted_messages)
    }

    pub async fn append_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &Vec<send_messages::Message>,
    ) -> Result<(), Error> {
        let mut received_messages = Vec::with_capacity(messages.len());
        for message in messages {
            let encrypted_message;
            let message = match self.cipher {
                Some(ref cipher) => {
                    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
                    let encrypted_payload =
                        cipher.encrypt(&nonce, message.payload.as_ref()).unwrap();
                    let payload = [&nonce, encrypted_payload.as_slice()].concat();
                    encrypted_message = send_messages::Message {
                        id: message.id,
                        length: payload.len() as u32,
                        payload: Bytes::from(payload),
                    };
                    &encrypted_message
                }
                None => message,
            };
            received_messages.push(Message::from_message(message));
        }
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        topic.append_messages(partitioning, received_messages).await
    }
}
