use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::{panicking, sleep};
use std::time::Duration;

use assert_cmd::prelude::CommandCargoExt;
use async_trait::async_trait;
use derive_more::Display;
use futures::executor::block_on;
use uuid::Uuid;

use iggy::client::{Client, StreamClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::identifier::Identifier;
use iggy::models::identity_info::IdentityInfo;
use iggy::models::permissions::{GlobalPermissions, Permissions};
use iggy::models::user_status::UserStatus::Active;
use iggy::users::defaults::*;
use server::configs::config_provider::{ConfigProvider, FileConfigProvider};

pub const SYSTEM_PATH_ENV_VAR: &str = "IGGY_SYSTEM_PATH";
pub const TEST_VERBOSITY_ENV_VAR: &str = "IGGY_TEST_VERBOSE";
pub const IPV6_ENV_VAR: &str = "IGGY_TCP_IPV6";
const USER_PASSWORD: &str = "secret";
const SLEEP_INTERVAL_MS: u64 = 20;
const LOCAL_DATA_PREFIX: &str = "local_data_";

const MAX_PORT_WAIT_DURATION_S: u64 = 60;

#[derive(PartialEq)]
pub enum IpAddrKind {
    V4,
    V6,
}

#[async_trait]
pub trait ClientFactory: Sync + Send {
    async fn create_client(&self) -> Box<dyn Client>;
}

#[derive(Debug, Clone, Copy, PartialEq, Display)]
pub enum Transport {
    #[display("http")]
    Http,

    #[display("quic")]
    Quic,

    #[display("tcp")]
    Tcp,
}

#[derive(Display, Debug)]
enum ServerProtocolAddr {
    #[display("RAW_TCP:{_0}")]
    RawTcp(SocketAddr),

    #[display("HTTP_TCP:{_0}")]
    HttpTcp(SocketAddr),

    #[display("QUIC_UDP:{_0}")]
    QuicUdp(SocketAddr),
}

#[derive(Debug)]
pub struct TestServer {
    local_data_path: String,
    envs: HashMap<String, String>,
    child_handle: Option<Child>,
    server_addrs: Vec<ServerProtocolAddr>,
    stdout_file_path: Option<PathBuf>,
    stderr_file_path: Option<PathBuf>,
    cleanup: bool,
    server_executable_path: Option<String>,
}

impl TestServer {
    pub fn new(
        extra_envs: Option<HashMap<String, String>>,
        cleanup: bool,
        server_executable_path: Option<String>,
        ip_kind: IpAddrKind,
    ) -> Self {
        let mut envs = HashMap::new();
        if let Some(extra) = extra_envs {
            for (key, value) in extra {
                envs.insert(key, value);
            }
        }

        if ip_kind == IpAddrKind::V6 {
            envs.insert(IPV6_ENV_VAR.to_string(), "true".to_string());
        }

        // If IGGY_SYSTEM_PATH is not set, use a random path starting with "local_data_"
        let local_data_path = if let Some(system_path) = envs.get(SYSTEM_PATH_ENV_VAR) {
            system_path.to_string()
        } else {
            TestServer::get_random_path()
        };

        Self::create(
            local_data_path,
            envs,
            cleanup,
            server_executable_path,
            ip_kind,
        )
    }

    pub fn create(
        local_data_path: String,
        envs: HashMap<String, String>,
        cleanup: bool,
        server_executable_path: Option<String>,
        ip_kind: IpAddrKind,
    ) -> Self {
        let mut server_addrs = Vec::new();

        if let Some(tcp_addr) = envs.get("IGGY_TCP_ADDRESS") {
            server_addrs.push(ServerProtocolAddr::RawTcp(tcp_addr.parse().unwrap()));
        }

        if let Some(http_addr) = envs.get("IGGY_HTTP_ADDRESS") {
            server_addrs.push(ServerProtocolAddr::HttpTcp(http_addr.parse().unwrap()));
        }

        if let Some(quic_addr) = envs.get("IGGY_QUIC_ADDRESS") {
            server_addrs.push(ServerProtocolAddr::QuicUdp(quic_addr.parse().unwrap()));
        }

        if server_addrs.is_empty() {
            server_addrs = match ip_kind {
                IpAddrKind::V6 => Self::get_server_ipv6_addrs_with_random_port(),
                _ => Self::get_server_ipv4_addrs_with_random_port(),
            }
        }

        Self {
            local_data_path,
            envs,
            child_handle: None,
            server_addrs,
            stdout_file_path: None,
            stderr_file_path: None,
            cleanup,
            server_executable_path,
        }
    }

    pub fn start(&mut self) {
        self.set_server_addrs_from_env();
        self.cleanup();
        let files_path = self.local_data_path.clone();
        let mut command = if let Some(server_executable_path) = &self.server_executable_path {
            Command::new(server_executable_path)
        } else {
            Command::cargo_bin("iggy-server").unwrap()
        };
        command.env(SYSTEM_PATH_ENV_VAR, files_path);
        command.envs(self.envs.clone());

        // By default, server all logs are redirected to files,
        // and dumped to stderr when test fails. With IGGY_TEST_VERBOSE=1
        // logs are dumped to stdout during test execution.
        if std::env::var(TEST_VERBOSITY_ENV_VAR).is_ok()
            || self.envs.contains_key(TEST_VERBOSITY_ENV_VAR)
        {
            command.stdout(Stdio::inherit());
            command.stderr(Stdio::inherit());
        } else {
            command.stdout(self.get_stdout_file());
            self.stdout_file_path = Some(fs::canonicalize(self.get_stdout_file_path()).unwrap());
            command.stderr(self.get_stderr_file());
            self.stderr_file_path = Some(fs::canonicalize(self.get_stderr_file_path()).unwrap());
        }

        let child = command.spawn().unwrap();
        self.child_handle = Some(child);

        if self.child_handle.as_ref().unwrap().stdout.is_some() {
            let child_stdout = self.child_handle.as_mut().unwrap().stdout.take().unwrap();
            std::thread::spawn(move || {
                let reader = BufReader::new(child_stdout);
                for line in reader.lines() {
                    println!("{}", line.unwrap());
                }
            });
        }
        if self.child_handle.as_ref().unwrap().stderr.is_some() {
            let child_stderr = self.child_handle.as_mut().unwrap().stderr.take().unwrap();
            std::thread::spawn(move || {
                let reader = BufReader::new(child_stderr);
                for line in reader.lines() {
                    eprintln!("{}", line.unwrap());
                }
            });
        }
        self.wait_until_server_has_bound();
    }

    pub fn stop(&mut self) {
        #[allow(unused_mut)]
        if let Some(mut child_handle) = self.child_handle.take() {
            #[cfg(unix)]
            unsafe {
                use libc::kill;
                use libc::SIGTERM;
                kill(child_handle.id() as libc::pid_t, SIGTERM);
            }

            #[cfg(not(unix))]
            child_handle.kill().unwrap();

            if let Ok(output) = child_handle.wait_with_output() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                if let Some(stderr_file_path) = &self.stderr_file_path {
                    OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(stderr_file_path)
                        .unwrap()
                        .write_all(stderr.as_bytes())
                        .unwrap();
                }

                if let Some(stdout_file_path) = &self.stdout_file_path {
                    OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(stdout_file_path)
                        .unwrap()
                        .write_all(stdout.as_bytes())
                        .unwrap();
                }
            }
        }
        self.cleanup();
    }

    pub fn is_started(&self) -> bool {
        self.child_handle.is_some()
    }

    pub fn pid(&self) -> u32 {
        self.child_handle.as_ref().unwrap().id()
    }

    fn cleanup(&self) {
        if !self.cleanup {
            return;
        }

        if fs::metadata(&self.local_data_path).is_ok() {
            fs::remove_dir_all(&self.local_data_path).unwrap();
        }
    }

    fn get_server_ipv4_addrs_with_random_port() -> Vec<ServerProtocolAddr> {
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        vec![
            ServerProtocolAddr::QuicUdp(addr),
            ServerProtocolAddr::RawTcp(addr),
            ServerProtocolAddr::HttpTcp(addr),
        ]
    }

    fn get_server_ipv6_addrs_with_random_port() -> Vec<ServerProtocolAddr> {
        let addr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0);
        vec![
            ServerProtocolAddr::QuicUdp(addr),
            ServerProtocolAddr::RawTcp(addr),
            ServerProtocolAddr::HttpTcp(addr),
        ]
    }

    fn set_server_addrs_from_env(&mut self) {
        for server_protocol_addr in &self.server_addrs {
            let key = match server_protocol_addr {
                ServerProtocolAddr::RawTcp(addr) => {
                    ("IGGY_TCP_ADDRESS".to_string(), addr.to_string())
                }
                ServerProtocolAddr::HttpTcp(addr) => {
                    ("IGGY_HTTP_ADDRESS".to_string(), addr.to_string())
                }
                ServerProtocolAddr::QuicUdp(addr) => {
                    ("IGGY_QUIC_ADDRESS".to_string(), addr.to_string())
                }
            };

            self.envs.entry(key.0).or_insert(key.1);
        }
    }

    fn wait_until_server_has_bound(&mut self) {
        let config_path = format!("{}/runtime/current_config.toml", self.local_data_path);
        let file_config_provider = FileConfigProvider::new(config_path.clone());

        let max_attempts = (MAX_PORT_WAIT_DURATION_S * 1000) / SLEEP_INTERVAL_MS;
        self.server_addrs.clear();

        let config = block_on(async {
            let mut loaded_config = None;

            for _ in 0..max_attempts {
                if !Path::new(&config_path).exists() {
                    if let Some(exit_status) =
                        self.child_handle.as_mut().unwrap().try_wait().unwrap()
                    {
                        panic!("Server process has exited with status {}!", exit_status);
                    }
                    sleep(Duration::from_millis(SLEEP_INTERVAL_MS));
                    continue;
                }
                match file_config_provider.load_config().await {
                    Ok(config) => {
                        loaded_config = Some(config);
                        break;
                    }
                    Err(_) => sleep(Duration::from_millis(SLEEP_INTERVAL_MS)),
                }
            }
            loaded_config
        });

        if let Some(config) = config {
            self.server_addrs.push(ServerProtocolAddr::QuicUdp(
                config.quic.address.parse().unwrap(),
            ));

            self.server_addrs.push(ServerProtocolAddr::RawTcp(
                config.tcp.address.parse().unwrap(),
            ));

            self.server_addrs.push(ServerProtocolAddr::HttpTcp(
                config.http.address.parse().unwrap(),
            ));
        } else {
            panic!(
                "Failed to load config from file {} in {} s!",
                config_path, MAX_PORT_WAIT_DURATION_S
            );
        }
    }

    fn get_stdout_file_path(&self) -> PathBuf {
        format!("{}_stdout.txt", self.local_data_path).into()
    }

    fn get_stderr_file_path(&self) -> PathBuf {
        format!("{}_stderr.txt", self.local_data_path).into()
    }

    fn get_stdout_file(&self) -> File {
        File::create(self.get_stdout_file_path()).unwrap()
    }

    fn get_stderr_file(&self) -> File {
        File::create(self.get_stderr_file_path()).unwrap()
    }

    fn read_file_to_string(path: &str) -> String {
        fs::read_to_string(path).unwrap()
    }

    pub fn get_local_data_path(&self) -> &str {
        &self.local_data_path
    }

    pub fn get_random_path() -> String {
        format!("{}{}", LOCAL_DATA_PREFIX, Uuid::now_v7().to_u128_le())
    }

    pub fn get_http_api_addr(&self) -> Option<String> {
        for server_protocol_addr in &self.server_addrs {
            if let ServerProtocolAddr::HttpTcp(a) = server_protocol_addr {
                return Some(a.to_string());
            }
        }
        None
    }

    pub fn get_raw_tcp_addr(&self) -> Option<String> {
        for server_protocol_addr in &self.server_addrs {
            if let ServerProtocolAddr::RawTcp(a) = server_protocol_addr {
                return Some(a.to_string());
            }
        }
        None
    }

    pub fn get_quic_udp_addr(&self) -> Option<String> {
        for server_protocol_addr in &self.server_addrs {
            if let ServerProtocolAddr::QuicUdp(a) = server_protocol_addr {
                return Some(a.to_string());
            }
        }
        None
    }

    pub fn get_server_ip_addr(&self) -> Option<String> {
        if let Some(server_address) = self
            .get_raw_tcp_addr()
            .or_else(|| self.get_http_api_addr())
            .or_else(|| self.get_quic_udp_addr())
        {
            server_address
                .split(':')
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .first()
                .cloned()
        } else {
            None
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
        if panicking() {
            if let Some(stdout_file_path) = &self.stdout_file_path {
                eprintln!(
                    "Iggy server stdout:\n{}",
                    Self::read_file_to_string(stdout_file_path.to_str().unwrap())
                );
            }

            if let Some(stderr_file_path) = &self.stderr_file_path {
                eprintln!(
                    "Iggy server stderr:\n{}",
                    Self::read_file_to_string(stderr_file_path.to_str().unwrap())
                );
            }
        }
        if let Some(stdout_file_path) = &self.stdout_file_path {
            fs::remove_file(stdout_file_path).unwrap();
        }
        if let Some(stderr_file_path) = &self.stderr_file_path {
            fs::remove_file(stderr_file_path).unwrap();
        }
    }
}

impl Default for TestServer {
    fn default() -> Self {
        TestServer::new(None, true, None, IpAddrKind::V4)
    }
}

pub async fn create_user(client: &IggyClient, username: &str) {
    client
        .create_user(
            username,
            USER_PASSWORD,
            Active,
            Some(Permissions {
                global: GlobalPermissions {
                    manage_servers: true,
                    read_servers: true,
                    manage_users: true,
                    read_users: true,
                    manage_streams: true,
                    read_streams: true,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                },
                streams: None,
            }),
        )
        .await
        .unwrap();
}

pub async fn delete_user(client: &IggyClient, username: &str) {
    client
        .delete_user(&Identifier::named(username).unwrap())
        .await
        .unwrap();
}

pub async fn login_root(client: &IggyClient) -> IdentityInfo {
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap()
}

pub async fn login_user(client: &IggyClient, username: &str) -> IdentityInfo {
    client.login_user(username, USER_PASSWORD).await.unwrap()
}

pub async fn assert_clean_system(system_client: &IggyClient) {
    let streams = system_client.get_streams().await.unwrap();
    assert!(streams.is_empty());

    let users = system_client.get_users().await.unwrap();
    assert_eq!(users.len(), 1);
}
