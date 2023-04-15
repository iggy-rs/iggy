use crate::error::Error;
use crate::message::Message;
use crate::partitions::partition::Partition;
use crate::segments::segment::Segment;
use tracing::info;

impl Partition {
    pub fn get_messages(&self, offset: u64, count: u32) -> Option<Vec<&Message>> {
        if self.segments.is_empty() {
            return None;
        }

        let mut end_offset = offset + (count - 1) as u64;
        let max_offset = self.segments.last().unwrap().current_offset;
        if end_offset > max_offset {
            end_offset = max_offset;
        }

        let segments = self
            .segments
            .iter()
            .filter(|segment| {
                (segment.start_offset >= offset && segment.current_offset <= end_offset)
                    || (segment.start_offset <= offset && segment.current_offset >= offset)
                    || (segment.start_offset <= end_offset && segment.current_offset >= end_offset)
            })
            .collect::<Vec<&Segment>>();

        if segments.is_empty() {
            return None;
        }

        let mut messages = Vec::new();
        for segment in segments {
            let segment_messages = segment.get_messages(offset, count);
            if segment_messages.is_none() {
                continue;
            }

            for message in segment_messages.unwrap() {
                messages.push(message);
            }
        }

        Some(messages)
    }

    pub async fn append_messages(&mut self, message: Message) -> Result<(), Error> {
        let segment = self.segments.last_mut();
        if segment.is_none() {
            return Err(Error::SegmentNotFound);
        }

        let segment = segment.unwrap();
        if segment.is_full() {
            info!(
                "Current segment is full, creating new segment for partition with ID: {}",
                self.id
            );
            let start_offset = segment.end_offset + 1;
            let mut new_segment = Segment::create(
                self.id,
                start_offset,
                &self.path,
                self.config.segment.clone(),
            );
            new_segment.persist().await?;
            self.segments.push(new_segment);
            self.segments
                .sort_by(|a, b| a.start_offset.cmp(&b.start_offset));
        }

        let segment = self.segments.last_mut();
        segment.unwrap().append_messages(message).await
    }
}
