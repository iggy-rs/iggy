use sdk::error::Error;
use std::collections::HashMap;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct ConsumerGroup {
    pub topic_id: u32,
    pub id: u32,
    pub partitions_count: u32,
    pub members: HashMap<u32, RwLock<ConsumerGroupMember>>,
}

#[derive(Debug)]
pub struct ConsumerGroupMember {
    pub id: u32,
    partitions: HashMap<u32, u32>,
    current_partition_index: u32,
}

impl ConsumerGroup {
    pub async fn calculate_partition_id(&mut self, member_id: u32) -> Result<u32, Error> {
        let member = self.members.get_mut(&member_id);
        if let Some(member) = member {
            return Ok(member.write().await.calculate_partition_id());
        }
        Err(Error::ConsumerGroupMemberNotFound(self.topic_id, member_id))
    }

    pub fn add_member(&mut self, member_id: u32) {
        self.members.insert(
            member_id,
            RwLock::new(ConsumerGroupMember {
                id: member_id,
                partitions: HashMap::new(),
                current_partition_index: 0,
            }),
        );
        self.assign_partitions();
    }

    pub fn delete_member(&mut self, member_id: u32) {
        if self.members.remove(&member_id).is_some() {
            self.assign_partitions();
        }
    }

    fn assign_partitions(&mut self) {
        // TODO: Implement assigning partitions to members
    }
}

impl ConsumerGroupMember {
    pub fn calculate_partition_id(&mut self) -> u32 {
        let partition_index = self.current_partition_index;
        let partition_id = *self.partitions.get(&partition_index).unwrap();
        if self.partitions.len() == (partition_index + 1) as usize {
            self.current_partition_index = 0;
        } else {
            self.current_partition_index += 1;
        }
        partition_id
    }
}
