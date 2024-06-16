use std::{
    collections::HashMap,
    fs::{create_dir, remove_dir_all},
    path::Path,
    sync::{Arc, OnceLock},
    time::Instant,
};
use sysinfo::{Pid, System as SysinfoSystem};
use tracing::info;

use crate::{
    configs::{server::PersonalAccessTokenConfig, system::SystemConfig, tcp::TcpConfig},
    streaming::{
        clients::client_manager::ClientManager, diagnostics::metrics::Metrics,
        persistence::persister::StoragePersister, storage::SystemStorage, streams::stream::Stream,
        users::permissioner::Permissioner,
    },
    tpc::{
        connector::{Receiver, ShardConnector, StopReceiver, StopSender},
        utils::hash_string,
    },
};

use super::shard_frame::ShardFrame;
use fast_async_mutex::mutex::Mutex;
use flume::SendError;
use iggy::{
    error::IggyError,
    locking::{IggySharedMut, IggySharedMutFn},
    models::stats::Stats,
    utils::crypto::{Aes256GcmEncryptor, Encryptor},
};
use sled::Db;
use sysinfo::System;
pub const SHARD_NAME: &str = "iggy_shard";

pub struct IggyShard {
    pub id: u16,
    pub hash: u32,
    shards: Vec<Shard>,

    pub(crate) permissioner: Permissioner,
    pub(crate) storage: Arc<SystemStorage>,
    pub(crate) streams: HashMap<u32, Stream>,
    pub(crate) streams_ids: HashMap<String, u32>,
    // TODO - get rid of this dynamic dispatch.
    encryptor: Option<Box<dyn Encryptor>>,
    pub(crate) system_config: Arc<SystemConfig>,
    pub(crate) tcp_config: Arc<TcpConfig>,
    client_manager: IggySharedMut<ClientManager>,
    pub(crate) metrics: Metrics,
    db: Arc<Db>,
    pat_config: PersonalAccessTokenConfig,
    message_receiver: Receiver<ShardFrame>,
    stop_receiver: StopReceiver,
    stop_sender: StopSender,
}

impl IggyShard {
    pub fn new(
        id: u16,
        shards: Vec<Shard>,
        system_config: Arc<SystemConfig>,
        tcp_config: TcpConfig,
        pat_config: PersonalAccessTokenConfig,
        db: Arc<Db>,
        storage: Arc<SystemStorage>,
        shard_messages_receiver: Receiver<ShardFrame>,
        stop_receiver: StopReceiver,
        stop_sender: StopSender,
    ) -> Self {
        let name = &format!("{}_{}", SHARD_NAME, id);
        Self {
            id,
            hash: hash_string(&name).unwrap(),
            shards,
            permissioner: Permissioner::default(),
            storage,
            streams: HashMap::new(),
            streams_ids: HashMap::new(),
            encryptor: match system_config.encryption.enabled {
                true => Some(Box::new(
                    Aes256GcmEncryptor::from_base64_key(&system_config.encryption.key).unwrap(),
                )),
                false => None,
            },
            system_config,
            tcp_config,
            client_manager: IggySharedMut::new(ClientManager::default()),
            metrics: Metrics::init(),
            db,
            pat_config,
            message_receiver: shard_messages_receiver,
            stop_receiver,
            stop_sender,
        }
    }

    pub async fn init(&mut self) -> Result<(), IggyError> {
        info!(
            "Initializing system, data will be stored at: {}",
            self.system_config.get_system_path()
        );
        let now = Instant::now();
        self.load_version().await?;
        self.load_users().await?;
        self.load_streams().await?;
        info!("Initialized system in {} ms.", now.elapsed().as_millis());
        Ok(())
    }

    pub async fn stop(self) -> Result<(), SendError<()>> {
        self.stop_sender.send_async(()).await?;
        Ok(())
    }
}

pub struct Shard {
    hash: u32,
    connection: ShardConnector<ShardFrame>,
}

impl Shard {
    pub fn new(name: String, connection: ShardConnector<ShardFrame>) -> Self {
        let hash = hash_string(&name).unwrap();
        Self { hash, connection }
    }
}

impl IggyShard {
    pub async fn get_stats_bypass_auth(&self) -> Result<Stats, IggyError> {
        fn sysinfo() -> &'static Mutex<SysinfoSystem> {
            static SYSINFO: OnceLock<Mutex<SysinfoSystem>> = OnceLock::new();
            SYSINFO.get_or_init(|| {
                let mut sys = SysinfoSystem::new_all();
                sys.refresh_all();
                Mutex::new(sys)
            })
        }

        let mut sys = sysinfo().lock().await;
        let process_id = std::process::id();
        sys.refresh_cpu();
        sys.refresh_memory();
        sys.refresh_process(Pid::from_u32(process_id));

        let total_cpu_usage = sys.global_cpu_info().cpu_usage();
        let total_memory = sys.total_memory().into();
        let available_memory = sys.available_memory().into();
        let clients_count = self.client_manager.read().await.get_clients().len() as u32;
        let hostname = sysinfo::System::host_name().unwrap_or("unknown_hostname".to_string());
        let os_name = sysinfo::System::name().unwrap_or("unknown_os_name".to_string());
        let os_version =
            sysinfo::System::long_os_version().unwrap_or("unknown_os_version".to_string());
        let kernel_version =
            sysinfo::System::kernel_version().unwrap_or("unknown_kernel_version".to_string());

        let mut stats = Stats {
            process_id,
            total_cpu_usage,
            total_memory,
            available_memory,
            clients_count,
            hostname,
            os_name,
            os_version,
            kernel_version,
            ..Default::default()
        };

        if let Some(process) = sys
            .processes()
            .values()
            .find(|p| p.pid() == Pid::from_u32(process_id))
        {
            stats.process_id = process.pid().as_u32();
            stats.cpu_usage = process.cpu_usage();
            stats.memory_usage = process.memory().into();
            stats.run_time = process.run_time().into();
            stats.start_time = process.start_time().into();

            let disk_usage = process.disk_usage();
            stats.read_bytes = disk_usage.total_read_bytes.into();
            stats.written_bytes = disk_usage.total_written_bytes.into();
        }

        drop(sys);

        for stream in self.streams.values() {
            stats.messages_count += stream.get_messages_count();
            stats.segments_count += stream.get_segments_count();
            stats.messages_size_bytes += stream.get_size();
            stats.streams_count += 1;
            stats.topics_count += stream.topics.len() as u32;
            stats.partitions_count += stream
                .topics
                .values()
                .map(|t| t.partitions.len() as u32)
                .sum::<u32>();
            stats.consumer_groups_count += stream
                .topics
                .values()
                .map(|t| t.consumer_groups.len() as u32)
                .sum::<u32>();
        }

        Ok(stats)
    }
}
