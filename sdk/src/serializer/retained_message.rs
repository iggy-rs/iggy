use crate::bytes_serializable::BytesSerializable;
use crate::models::header;
use crate::models::header::{HeaderKey, HeaderValue};
use crate::models::messages::{MessageState, RetainedMessage};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::{BufMut, BytesMut};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

impl Serialize for RetainedMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CustomBytes", 7)?;
        state.serialize_field("offset", &self.get_offset())?;
        state.serialize_field("state", &self.get_message_state())?;
        state.serialize_field("timestamp", &self.get_timestamp())?;
        state.serialize_field("id", &self.get_id())?;
        state.serialize_field("checksum", &self.get_checksum())?;
        state.serialize_field("headers", &self.try_get_headers().unwrap_or_default())?;
        state.serialize_field("payload", &STANDARD.encode(self.get_payload()))?;
        state.end()
    }
}
struct RetainedMessageVisitor;

impl<'de> serde::de::Visitor<'de> for RetainedMessageVisitor {
    type Value = RetainedMessage;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("struct RetainedMessage")
    }

    fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
    where
        V: serde::de::MapAccess<'de>,
    {
        let mut offset: Option<u64> = None;
        let mut state: Option<&str> = None;
        let mut timestamp: Option<u64> = None;
        let mut id: Option<u128> = None;
        let mut checksum: Option<u32> = None;
        let mut headers: Option<HashMap<HeaderKey, HeaderValue>> = None;
        let mut payload: Option<&str> = None;

        while let Some(key) = map.next_key()? {
            match key {
                "offset" => {
                    if offset.is_some() {
                        return Err(serde::de::Error::duplicate_field("offset"));
                    }
                    offset = Some(map.next_value()?);
                }
                "state" => {
                    if state.is_some() {
                        return Err(serde::de::Error::duplicate_field("state"));
                    }
                    state = Some(map.next_value()?);
                }
                "timestamp" => {
                    if timestamp.is_some() {
                        return Err(serde::de::Error::duplicate_field("timestamp"));
                    }
                    timestamp = Some(map.next_value()?);
                }
                "id" => {
                    if id.is_some() {
                        return Err(serde::de::Error::duplicate_field("id"));
                    }
                    id = Some(map.next_value()?);
                }
                "checksum" => {
                    if checksum.is_some() {
                        return Err(serde::de::Error::duplicate_field("checksum"));
                    }
                    checksum = Some(map.next_value()?);
                }
                "headers" => {
                    if headers.is_some() {
                        return Err(serde::de::Error::duplicate_field("headers"));
                    }
                    headers = Some(map.next_value()?);
                }
                "payload" => {
                    if payload.is_some() {
                        return Err(serde::de::Error::duplicate_field("payload"));
                    }
                    payload = Some(map.next_value()?);
                }
                _ => {
                    return Err(serde::de::Error::unknown_field(
                        "unknown field",
                        &["offset", "state", "timestamp", "id", "checksum", "payload"],
                    ));
                }
            }
        }

        let offset = offset.ok_or_else(|| serde::de::Error::missing_field("offset"))?;
        let state = state.ok_or_else(|| serde::de::Error::missing_field("state"))?;
        let timestamp = timestamp.ok_or_else(|| serde::de::Error::missing_field("timestamp"))?;
        let id = id.ok_or_else(|| serde::de::Error::missing_field("id"))?;
        let checksum = checksum.ok_or_else(|| serde::de::Error::missing_field("checksum"))?;
        let message_payload = payload.ok_or_else(|| serde::de::Error::missing_field("payload"))?;
        let message_payload = STANDARD.decode(message_payload.as_bytes()).unwrap();
        let mut payload = Vec::with_capacity(
            (crate::models::messages::POLLED_MESSAGE_METADATA
                + message_payload.len() as u32
                + header::get_headers_size_bytes(&headers)) as usize,
        );

        payload.put_u64_le(offset);
        payload.put_u8(MessageState::from_str(state).unwrap().as_code());
        payload.put_u64_le(timestamp);
        payload.put_u128_le(id);
        payload.put_u32_le(checksum);
        if let Some(headers) = headers {
            let headers_bytes = headers.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            payload.put_u32_le(headers_bytes.len() as u32);
            payload.put_slice(&headers_bytes);
        } else {
            payload.put_u32_le(0u32);
        }
        payload.put_u32_le(message_payload.len() as u32);
        payload.put_slice(&message_payload);
        Ok(RetainedMessage {
            length: payload.len() as u32,
            bytes: payload,
        })
    }
}

impl<'de> Deserialize<'de> for RetainedMessage {
    fn deserialize<D>(deserializer: D) -> Result<RetainedMessage, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(RetainedMessageVisitor)
    }
}
#[cfg(test)]
mod tests {
    use crate::messages::send_messages;
    use crate::models::header::{HeaderKey, HeaderValue};
    use crate::models::messages::{MessageState, RetainedMessage};
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::str::FromStr;

    fn create_headers(count: u32) -> HashMap<HeaderKey, HeaderValue> {
        let mut headers = HashMap::new();
        for i in 1..=count {
            headers.insert(
                HeaderKey::new(&format!("key_{}", i)).unwrap(),
                HeaderValue::from_str(&format!("Value {}", i)).unwrap(),
            );
        }
        headers
    }
    #[test]
    fn should_serialize_and_deserialize_retained_message_properly() {
        let headers = create_headers(4);
        let payload_value = "test";
        let offset = 69;
        let id = 420;
        let length = payload_value.len() as u32;
        let payload = Bytes::from(payload_value);
        let headers = Some(headers);
        let message = RetainedMessage::from_message(
            offset,
            &send_messages::Message {
                id,
                length,
                payload: payload.clone(),
                headers: headers.clone(),
            },
        );

        let serialized = serde_json::to_string(&message).unwrap();
        let deserialized: RetainedMessage = serde_json::from_str(&serialized).unwrap();

        let message_offset = deserialized.get_offset();
        let message_state = deserialized.get_message_state();
        let message_id = deserialized.get_id();
        let message_headers = deserialized.try_get_headers().unwrap_or_default();
        let message_payload = deserialized.get_payload();

        assert_eq!(message_offset, offset);
        assert_eq!(message_state, MessageState::Available);
        assert_eq!(message_id, id);
        assert_eq!(message_headers, headers);
        assert_eq!(message_payload, &payload);
        assert_eq!(message_payload.len() as u32, length);
    }
    #[test]
    fn should_serialize_and_deserialize_multiple_messages_properly() {
        let messages_count = 20;
        let headers = create_headers(10);
        let payload_value = "test";
        let length = payload_value.len() as u32;
        let payload = Bytes::from(payload_value);
        let headers = Some(headers);
        let mut messages = Vec::new();

        for i in 1..=messages_count {
            let message = RetainedMessage::from_message(
                i,
                &send_messages::Message {
                    id: (i + 69) as u128,
                    length,
                    payload: payload.clone(),
                    headers: headers.clone(),
                },
            );
            messages.push(message);
        }

        let serialized = serde_json::to_string(&messages).unwrap();
        let deserialized: Vec<RetainedMessage> = serde_json::from_str(&serialized).unwrap();

        for (i, message) in deserialized.iter().enumerate() {
            let i = i + 1;
            let message_offset = message.get_offset();
            let message_state = message.get_message_state();
            let message_id = message.get_id();
            let message_headers = message.try_get_headers().unwrap_or_default();
            let message_payload = message.get_payload();

            assert_eq!(message_offset, i as u64);
            assert_eq!(message_state, MessageState::Available);
            assert_eq!(message_id, (i + 69) as u128);
            assert_eq!(message_headers, headers);
            assert_eq!(message_payload, &payload);
            assert_eq!(message_payload.len() as u32, length);
        }
    }
}
