use crate::config::{BackendConfig, LoadBalanceStrategy};
use crate::protocol::{ProxyProtocolV2, ProxyHeader, Protocol};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, TcpListener};
use tokio::time::{timeout, Duration};
use tracing::{info, error, warn};
use anyhow::Result;
use crate::load_balancer::LoadBalancer;

pub struct ProxyHandler {
    load_balancer: Arc<LoadBalancer>,
    enable_proxy_protocol: bool,
    connect_timeout: Duration,
    rw_timeout: Duration,
    tcp_listener: Arc<TcpListener>,
    // 连接计数器
    active_connections: Arc<AtomicUsize>,
    // 最大连接数，0 表示不限制
    max_connections: usize,
}

impl ProxyHandler {
    pub async fn new(
        bind_addr: SocketAddr,
        backends: Vec<BackendConfig>,
        strategy: LoadBalanceStrategy,
        enable_proxy_protocol: bool,
        connect_timeout: Duration,
        rw_timeout: Duration,
        max_connections: usize,
    ) -> Result<Self> {
        let listener = TcpListener::bind(bind_addr).await?;

        // 根据 max_connections 的值显示不同的日志信息
        if max_connections == 0 {
            info!("TCP 代理服务器启动，监听地址: {}, 连接数: 无限制", bind_addr);
        } else {
            info!("TCP 代理服务器启动，监听地址: {}, 最大连接数: {}", bind_addr, max_connections);
        }

        Ok(Self {
            load_balancer: Arc::new(LoadBalancer::new(backends, strategy)),
            enable_proxy_protocol,
            connect_timeout,
            rw_timeout,
            tcp_listener: Arc::new(listener),
            active_connections: Arc::new(AtomicUsize::new(0)),
            max_connections,
        })
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            match self.tcp_listener.accept().await {
                Ok((stream, addr)) => {
                    // 只有当 max_connections > 0 时才进行连接数管理
                    if self.max_connections > 0 {
                        let current_connections = self.active_connections.load(Ordering::Relaxed);
                        if current_connections >= self.max_connections {
                            warn!("达到最大连接数限制 ({}), 拒绝新连接 {}", self.max_connections, addr);
                            // 直接关闭连接，不处理
                            drop(stream);
                            continue;
                        }

                        // 增加连接计数
                        self.active_connections.fetch_add(1, Ordering::Relaxed);

                        let handler = self.clone();
                        tokio::spawn(async move {
                            // 使用 ConnectionGuard 确保连接结束时减少计数
                            let _guard = ConnectionGuard::new(Arc::clone(&handler.active_connections));

                            if let Err(e) = handler.handle_connection(stream, addr).await {
                                error!("处理 TCP 连接失败: {}", e);
                            }
                        });
                    } else {
                        // max_connections 为 0，不限制连接数，也不维护计数器
                        let handler = self.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handler.handle_connection(stream, addr).await {
                                error!("处理 TCP 连接失败: {}", e);
                            }
                        });
                    }
                }
                Err(e) => {
                    error!("接受 TCP 连接失败: {}", e);
                }
            }
        }
    }

    pub async fn handle_connection(&self, mut client_stream: TcpStream, client_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 选择后端服务器
        let backend = match self.load_balancer.select_backend() {
            Some(backend) => backend,
            None => {
                error!("没有可用的后端服务器，关闭客户端连接: {}", client_addr);
                let _ = client_stream.shutdown().await;
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
                error!("连接后端服务器失败 {}: {}，关闭客户端连接: {}", backend.address, e, client_addr);
                let _ = client_stream.shutdown().await;
                return Ok(());
            }
            Err(_) => {
                error!("连接后端服务器超时: {}，关闭客户端连接: {}", backend.address, client_addr);
                let _ = client_stream.shutdown().await;
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
                error!("发送 PROXY Protocol 头部失败: {}，关闭连接", e);
                let _ = client_stream.shutdown().await;
                let _ = backend_stream.shutdown().await;
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
        let client_addr = client_stream.peer_addr().ok();
        let backend_addr = backend_stream.peer_addr().ok();

        let (mut client_read, mut client_write) = client_stream.into_split();
        let (mut backend_read, mut backend_write) = backend_stream.into_split();

        // 双向数据转发
        let client_to_backend = async {
            let mut buffer = [0; 8192];
            loop {
                match timeout(self.rw_timeout, client_read.read(&mut buffer)).await {
                    Ok(Ok(0)) => {
                        // 客户端关闭连接
                        info!("客户端关闭连接: {:?}", client_addr);
                        break;
                    }
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
            // 关闭到后端的写连接
            let _ = backend_write.shutdown().await;
        };

        let backend_to_client = async {
            let mut buffer = [0; 8192];
            loop {
                match timeout(self.rw_timeout, backend_read.read(&mut buffer)).await {
                    Ok(Ok(0)) => {
                        // 后端关闭连接
                        info!("后端关闭连接: {:?}", backend_addr);
                        break;
                    }
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
            // 关闭到客户端的写连接
            let _ = client_write.shutdown().await;
        };

        // 等待任一方向的数据传输完成
        tokio::select! {
            _ = client_to_backend => {},
            _ = backend_to_client => {},
        }

        info!("数据转发结束: {:?} <-> {:?}", client_addr, backend_addr);
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
            active_connections: Arc::clone(&self.active_connections),
            max_connections: self.max_connections,
        }
    }
}

/// RAII 结构，确保连接结束时自动减少计数
struct ConnectionGuard {
    counter: Arc<AtomicUsize>,
}

impl ConnectionGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}