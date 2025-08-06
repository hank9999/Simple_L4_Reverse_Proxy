use crate::config::{BackendConfig, LoadBalanceStrategy};
use crate::protocol::{ProxyProtocolV2, ProxyHeader, Protocol};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, TcpListener};
use tokio::time::{timeout, Duration};
use tracing::{info, error, warn};
use anyhow::Result;

pub struct LoadBalancer {
    backends: Vec<BackendConfig>,
    strategy: LoadBalanceStrategy,
    current: AtomicUsize,
}

impl LoadBalancer {
    pub fn new(backends: Vec<BackendConfig>, strategy: LoadBalanceStrategy) -> Self {
        Self {
            backends,
            strategy,
            current: AtomicUsize::new(0),
        }
    }

    pub fn select_backend(&self) -> Option<&BackendConfig> {
        if self.backends.is_empty() {
            return None;
        }

        match self.strategy {
            LoadBalanceStrategy::RoundRobin => {
                let index = self.current.fetch_add(1, Ordering::Relaxed) % self.backends.len();
                Some(&self.backends[index])
            }
            LoadBalanceStrategy::WeightedRoundRobin => {
                // 简化版加权轮询实现
                let index = self.current.fetch_add(1, Ordering::Relaxed) % self.backends.len();
                Some(&self.backends[index])
            }
            LoadBalanceStrategy::LeastConnections => {
                // 简化实现，实际应该跟踪每个后端的连接数
                let index = self.current.fetch_add(1, Ordering::Relaxed) % self.backends.len();
                Some(&self.backends[index])
            }
        }
    }
}

pub struct ProxyHandler {
    load_balancer: Arc<LoadBalancer>,
    enable_proxy_protocol: bool,
    connect_timeout: Duration,
    rw_timeout: Duration,
    tcp_listener: Arc<TcpListener>,
}

impl ProxyHandler {
    pub async fn new(
        bind_addr: SocketAddr,
        backends: Vec<BackendConfig>,
        strategy: LoadBalanceStrategy,
        enable_proxy_protocol: bool,
        connect_timeout: Duration,
        rw_timeout: Duration,
    ) -> Result<Self> {
        let listener = TcpListener::bind(bind_addr).await?;
        info!("TCP 代理服务器启动，监听地址: {}", bind_addr);
        
        Ok(Self {
            load_balancer: Arc::new(LoadBalancer::new(backends, strategy)),
            enable_proxy_protocol,
            connect_timeout,
            rw_timeout,
            tcp_listener: Arc::new(listener),
        })
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            match self.tcp_listener.accept().await {
                Ok((stream, addr)) => {
                    let handler = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handler.handle_connection(stream, addr).await {
                            error!("处理 TCP 连接失败: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("接受 TCP 连接失败: {}", e);
                }
            }
        }
    }

    pub async fn handle_connection(&self, client_stream: TcpStream, client_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 选择后端服务器
        let backend = match self.load_balancer.select_backend() {
            Some(backend) => backend,
            None => {
                error!("没有可用的后端服务器");
                return Ok(());
            }
        };

        info!("代理连接: {} -> {}", client_addr, backend.address);

        // 连接到后端服务器
        let backend_stream = match timeout(
            self.connect_timeout,
            TcpStream::connect(backend.address)
        ).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                error!("连接后端服务器失败 {}: {}", backend.address, e);
                return Ok(());
            }
            Err(_) => {
                error!("连接后端服务器超时: {}", backend.address);
                return Ok(());
            }
        };

        let mut backend_stream = backend_stream;

        // 如果启用了 PROXY Protocol，发送头部信息
        if self.enable_proxy_protocol {
            let proxy_header = ProxyHeader {
                src_addr: client_addr,
                dst_addr: backend.address,
                protocol: Protocol::TCP,
            };

            let header_bytes = ProxyProtocolV2::generate_header(&proxy_header);
            if let Err(e) = backend_stream.write_all(&header_bytes).await {
                error!("发送 PROXY Protocol 头部失败: {}", e);
                return Ok(());
            }
        }

        // 开始双向数据转发
        if let Err(e) = self.relay_data(client_stream, backend_stream).await {
            warn!("数据转发错误: {}", e);
        }

        Ok(())
    }

    async fn relay_data(&self, client_stream: TcpStream, backend_stream: TcpStream) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (mut client_read, mut client_write) = client_stream.into_split();
        let (mut backend_read, mut backend_write) = backend_stream.into_split();

        // 双向数据转发
        let client_to_backend = async {
            let mut buffer = [0; 8192];
            loop {
                match timeout(self.rw_timeout, client_read.read(&mut buffer)).await {
                    Ok(Ok(0)) => break, // 连接关闭
                    Ok(Ok(n)) => {
                        if let Err(e) = timeout(self.rw_timeout, backend_write.write_all(&buffer[..n])).await {
                            error!("写入后端数据失败: {:?}", e);
                            break;
                        }
                    }
                    Ok(Err(e)) => {
                        error!("读取客户端数据失败: {}", e);
                        break;
                    }
                    Err(_) => {
                        warn!("读取客户端数据超时");
                        break;
                    }
                }
            }
        };

        let backend_to_client = async {
            let mut buffer = [0; 8192];
            loop {
                match timeout(self.rw_timeout, backend_read.read(&mut buffer)).await {
                    Ok(Ok(0)) => break, // 连接关闭
                    Ok(Ok(n)) => {
                        if let Err(e) = timeout(self.rw_timeout, client_write.write_all(&buffer[..n])).await {
                            error!("写入客户端数据失败: {:?}", e);
                            break;
                        }
                    }
                    Ok(Err(e)) => {
                        error!("读取后端数据失败: {}", e);
                        break;
                    }
                    Err(_) => {
                        warn!("读取后端数据超时");
                        break;
                    }
                }
            }
        };

        // 等待任一方向的数据传输完成
        tokio::select! {
            _ = client_to_backend => {},
            _ = backend_to_client => {},
        }

        Ok(())
    }
}

impl Clone for ProxyHandler {
    fn clone(&self) -> Self {
        Self {
            load_balancer: Arc::clone(&self.load_balancer),
            enable_proxy_protocol: self.enable_proxy_protocol,
            connect_timeout: self.connect_timeout,
            rw_timeout: self.rw_timeout,
            tcp_listener: Arc::clone(&self.tcp_listener),
        }
    }
}