use crate::state::command::EntryCommand;
use crate::state::models::CreatePersonalAccessTokenWithHash;
use crate::state::State;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::storage::SystemStorage;
use crate::streaming::streams::stream::Stream;
use crate::streaming::users::user::User;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::streams::create_stream::CreateStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::users::create_user::CreateUser;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::timestamp::IggyTimestamp;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::create_dir;
use tracing::{error, info};

pub async fn convert(
    state: Arc<dyn State>,
    storage: Arc<SystemStorage>,
    mut streams: Vec<Stream>,
    mut users: Vec<User>,
    personal_access_tokens: Vec<PersonalAccessToken>,
) -> Result<(), IggyError> {
    info!("Converting storage to new format");
    state.init().await?;
    streams.sort_by(|a, b| a.stream_id.cmp(&b.stream_id));
    users.sort_by(|a, b| a.id.cmp(&b.id));
    info!("Converting {} users", users.len());
    for user in users {
        state
            .apply(
                0,
                EntryCommand::CreateUser(CreateUser {
                    username: user.username,
                    password: user.password,
                    status: user.status,
                    permissions: user.permissions.clone(),
                }),
            )
            .await?;
    }

    info!(
        "Converting {} personal access tokens",
        personal_access_tokens.len()
    );
    for personal_access_token in personal_access_tokens {
        let now = IggyTimestamp::now();
        let mut expiry = IggyExpiry::NeverExpire;
        if let Some(expiry_at) = personal_access_token.expiry_at {
            if expiry_at.as_micros() <= now.as_micros() {
                continue;
            }
            expiry = IggyExpiry::ExpireDuration((expiry_at.as_micros() - now.as_micros()).into());
        }

        state
            .apply(
                personal_access_token.user_id,
                EntryCommand::CreatePersonalAccessToken(CreatePersonalAccessTokenWithHash {
                    command: CreatePersonalAccessToken {
                        name: personal_access_token.name,
                        expiry,
                    },
                    hash: personal_access_token.token,
                }),
            )
            .await?;
    }

    info!("Converting {} streams", streams.len());
    for stream in streams {
        state
            .apply(
                0,
                EntryCommand::CreateStream(CreateStream {
                    stream_id: Some(stream.stream_id),
                    name: stream.name,
                }),
            )
            .await?;

        info!(
            "Converting {} topics for stream with ID: {}",
            stream.topics.len(),
            stream.stream_id
        );
        for topic in stream.topics.into_values() {
            state
                .apply(
                    0,
                    EntryCommand::CreateTopic(CreateTopic {
                        stream_id: topic.stream_id.try_into()?,
                        topic_id: Some(topic.topic_id),
                        partitions_count: topic.partitions.len() as u32,
                        compression_algorithm: topic.compression_algorithm,
                        message_expiry: topic.message_expiry,
                        max_topic_size: topic.max_topic_size,
                        replication_factor: if topic.replication_factor > 0 {
                            Some(topic.replication_factor)
                        } else {
                            None
                        },
                        name: topic.name,
                    }),
                )
                .await?;

            info!(
                "Converting {} consumer groups for topic with ID: {}",
                topic.consumer_groups.len(),
                topic.topic_id,
            );
            for group in topic.consumer_groups.into_values() {
                let group = group.read().await;
                state
                    .apply(
                        0,
                        EntryCommand::CreateConsumerGroup(CreateConsumerGroup {
                            stream_id: stream.stream_id.try_into()?,
                            topic_id: topic.topic_id.try_into()?,
                            group_id: Some(group.group_id),
                            name: group.name.to_owned(),
                        }),
                    )
                    .await?;
            }

            info!(
                "Converting {} partitions for topic with ID: {}",
                topic.partitions.len(),
                topic.topic_id,
            );
            for partition in topic.partitions.into_values() {
                let partition = partition.read().await;

                if !Path::new(&partition.offsets_path).exists()
                    && create_dir(&partition.offsets_path).await.is_err()
                {
                    error!(
                "Failed to create offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.partition_id, partition.stream_id, partition.topic_id
            );
                    return Err(IggyError::CannotCreatePartition(
                        partition.partition_id,
                        partition.stream_id,
                        partition.topic_id,
                    ));
                }

                info!("Creating consumer offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}, path: {}",
                    partition.partition_id, partition.stream_id, partition.topic_id, partition.consumer_offsets_path);
                if !Path::new(&partition.consumer_offsets_path).exists()
                    && create_dir(&partition.consumer_offsets_path).await.is_err()
                {
                    error!(
                "Failed to create consumer offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.partition_id, partition.stream_id, partition.topic_id
            );
                    return Err(IggyError::CannotCreatePartition(
                        partition.partition_id,
                        partition.stream_id,
                        partition.topic_id,
                    ));
                }

                info!("Creating consumer group offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}, path: {}",
                    partition.partition_id, partition.stream_id, partition.topic_id, partition.consumer_group_offsets_path);
                if !Path::new(&partition.consumer_group_offsets_path).exists()
                    && create_dir(&partition.consumer_group_offsets_path)
                        .await
                        .is_err()
                {
                    error!(
                "Failed to create consumer group offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.partition_id, partition.stream_id, partition.topic_id
            );
                    return Err(IggyError::CannotCreatePartition(
                        partition.partition_id,
                        partition.stream_id,
                        partition.topic_id,
                    ));
                }

                info!("Converting {} consumer offsets for partition with ID: {} for stream with ID: {} and topic with ID: {}",
                    partition.consumer_offsets.len(), partition.partition_id, partition.stream_id, partition.topic_id);
                for offset in partition.consumer_offsets.iter() {
                    storage.partition.save_consumer_offset(&offset).await?;
                }

                info!("Converting {} consumer group offsets for partition with ID: {} for stream with ID: {} and topic with ID: {}",
                    partition.consumer_group_offsets.len(), partition.partition_id, partition.stream_id, partition.topic_id);
                for offset in partition.consumer_group_offsets.iter() {
                    storage.partition.save_consumer_offset(&offset).await?;
                }
            }
        }
    }
    info!("Conversion completed");
    Ok(())
}
