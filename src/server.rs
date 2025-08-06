use crate::config::{Config, ListenerConfig, ProtocolType, UdpConfig};
use crate::proxy::ProxyHandler;
use crate::udp_proxy::UdpProxyHandler;
use anyhow::Result;
use tokio::time::Duration;
use tracing::error;

pub struct ProxyServer {
    config: Config,
}

impl ProxyServer {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn run(&self) -> Result<()> {
        let mut handles = Vec::new();

        // 为每个监听器启动服务
        for listener_config in &self.config.listeners {
            let config = listener_config.clone();
            let global_config = self.config.global.clone();

            let handle = tokio::spawn(async move {
                match config.protocol {
                    ProtocolType::TCP => {
                        if let Err(e) = Self::run_tcp_listener(config, global_config).await {
                            error!("TCP 监听器运行错误: {}", e);
                        }
                    }
                    ProtocolType::UDP => {
                        if let Err(e) = Self::run_udp_listener(config, global_config).await {
                            error!("UDP 监听器运行错误: {}", e);
                        }
                    }
                }
            });

            handles.push(handle);
        }

        // 等待所有监听器完成
        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    async fn run_tcp_listener(config: ListenerConfig, global_config: crate::config::GlobalConfig) -> Result<()> {
        let tcp_handler = ProxyHandler::new(
            config.bind,
            config.backends,
            config.load_balance,
            config.enable_proxy_protocol,
            Duration::from_secs(global_config.connect_timeout),
            Duration::from_secs(global_config.read_write_timeout),
        ).await?;

        tcp_handler.run().await
    }

    async fn run_udp_listener(config: ListenerConfig, _global_config: crate::config::GlobalConfig) -> Result<()> {
        let udp_config = config.udp_config.unwrap_or_else(|| UdpConfig {
            session_timeout: 300,
            buffer_size: 65536,
            max_sessions: 10000,
        });

        let udp_handler = UdpProxyHandler::new(
            config.bind,
            config.backends,
            config.load_balance,
            config.enable_proxy_protocol,
            udp_config,
        ).await?;

        udp_handler.run().await
    }
}