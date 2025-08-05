use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// 监听配置
    pub listeners: Vec<ListenerConfig>,
    /// 全局设置
    pub global: GlobalConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    /// 监听地址
    pub bind: SocketAddr,
    /// 协议类型
    pub protocol: ProtocolType,
    /// 后端服务器列表
    pub backends: Vec<BackendConfig>,
    /// 是否启用 PROXY Protocol v2
    #[serde(default = "default_proxy_protocol")]
    pub enable_proxy_protocol: bool,
    /// 负载均衡策略
    #[serde(default)]
    pub load_balance: LoadBalanceStrategy,
    /// UDP 特定配置
    #[serde(default)]
    pub udp_config: Option<UdpConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolType {
    TCP,
    UDP,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpConfig {
    /// UDP 会话超时时间（秒）
    #[serde(default = "default_udp_session_timeout")]
    pub session_timeout: u64,
    /// UDP 缓冲区大小
    #[serde(default = "default_udp_buffer_size")]
    pub buffer_size: usize,
    /// 最大会话数
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendConfig {
    /// 后端服务器地址
    pub address: SocketAddr,
    /// 权重（用于加权轮询）
    #[serde(default = "default_weight")]
    pub weight: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// 检查间隔（秒）
    #[serde(default = "default_interval")]
    pub interval: u64,
    /// 超时时间（秒）
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoadBalanceStrategy {
    RoundRobin,
    WeightedRoundRobin,
    LeastConnections,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    /// 连接超时时间（秒）
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,
    /// 读写超时时间（秒）
    #[serde(default = "default_rw_timeout")]
    pub read_write_timeout: u64,
    /// 最大并发连接数
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

// 默认值函数
fn default_proxy_protocol() -> bool { true }
fn default_weight() -> u32 { 1 }
fn default_interval() -> u64 { 30 }
fn default_timeout() -> u64 { 5 }
fn default_connect_timeout() -> u64 { 10 }
fn default_rw_timeout() -> u64 { 30 }
fn default_max_connections() -> usize { 1000 }
fn default_udp_session_timeout() -> u64 { 300 }
fn default_udp_buffer_size() -> usize { 65536 }
fn default_max_sessions() -> usize { 10000 }

impl Default for LoadBalanceStrategy {
    fn default() -> Self {
        LoadBalanceStrategy::RoundRobin
    }
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}