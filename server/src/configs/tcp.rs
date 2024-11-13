use iggy::utils::{byte_size::IggyByteSize, duration::IggyDuration};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DisplayFromStr;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TcpConfig {
    pub enabled: bool,
    pub address: String,
    pub ipv6: bool,
    pub tls: TcpTlsConfig,
    pub socket: TcpSocketConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TcpTlsConfig {
    pub enabled: bool,
    pub certificate: String,
    pub password: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TcpSocketConfig {
    pub override_defaults: bool,
    pub recv_buffer_size: IggyByteSize,
    pub send_buffer_size: IggyByteSize,
    pub keepalive: bool,
    pub nodelay: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub linger: IggyDuration,
}
