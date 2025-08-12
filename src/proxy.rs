use crate::config::{BackendConfig, LoadBalanceStrategy};
use crate::protocol::{ProxyProtocolV2, ProxyHeader, Protocol};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, TcpListener};
use tokio::time::{timeout, Duration};
use tracing::{info, error, warn, debug};
use anyhow::Result;
use crate::load_balancer::LoadBalancer;
use socket2::TcpKeepalive;

pub struct ProxyHandler {
    load_balancer: Arc<LoadBalancer>,
    enable_proxy_protocol: bool,
    connect_timeout: Duration,
    idle_timeout: Duration,
    tcp_listener: Arc<TcpListener>,
    bind_addr: SocketAddr,
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
        idle_timeout: Duration,
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
            idle_timeout,
            tcp_listener: Arc::new(listener),
            bind_addr,
            active_connections: Arc::new(AtomicUsize::new(0)),
            max_connections,
        })
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            match self.tcp_listener.accept().await {
                Ok((stream, addr)) => {
                    // 只有当 max_connections > 0 时才进行连接数管理
                    let use_connection_limit = self.max_connections > 0;
                    if use_connection_limit {
                        let current_connections = self.active_connections.load(Ordering::Relaxed);
                        if current_connections >= self.max_connections {
                            warn!("达到最大连接数限制 ({}), 拒绝新连接 {}", self.max_connections, addr);
                            drop(stream);
                            continue;
                        }
                        self.active_connections.fetch_add(1, Ordering::Relaxed);
                    }
                    let handler = self.clone();
                    tokio::spawn(async move {
                        // 使用 ConnectionGuard 确保连接结束时减少计数
                        let _guard = use_connection_limit.then(||
                            ConnectionGuard::new(Arc::clone(&handler.active_connections))
                        );

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

    /// 配置 TCP keepalive 实现空闲超时
    fn configure_tcp_keepalive(&self, stream: &TcpStream) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 如果空闲超时为 0，不设置 keepalive
        if self.idle_timeout.as_secs() == 0 {
            return Ok(());
        }

        let socket = socket2::SockRef::from(stream);

        // 配置 TCP keepalive 参数
        let keepalive = TcpKeepalive::new()
            // 设置空闲多久后开始发送 keepalive 探测包
            .with_time(self.idle_timeout)
            // 设置探测包之间的间隔（例如：每 10 秒发送一次）
            .with_interval(Duration::from_secs(10));

        // 在不同平台上设置探测次数
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let keepalive = keepalive.with_retries(3);  // 3 次探测失败后断开连接

        // 应用 keepalive 设置
        socket.set_tcp_keepalive(&keepalive)?;

        Ok(())
    }

    pub async fn handle_connection(&self, mut client_stream: TcpStream, client_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 为客户端连接配置 keepalive
        if let Err(e) = self.configure_tcp_keepalive(&client_stream) {
            warn!("配置客户端 TCP keepalive 失败: {}", e);
        }

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

        // 为后端连接配置 keepalive
        if let Err(e) = self.configure_tcp_keepalive(&backend_stream) {
            warn!("配置后端 TCP keepalive 失败: {}", e);
        }

        // 如果启用了 PROXY Protocol，发送头部信息
        if self.enable_proxy_protocol {
            // 尝试获取客户端连接的本地地址，失败则使用绑定地址
            let dst_addr = client_stream.local_addr().unwrap_or(self.bind_addr);

            let proxy_header = ProxyHeader {
                src_addr: client_addr,
                dst_addr,
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

    /// 使用 copy_bidirectional 数据转发，通过 TCP keepalive 实现空闲超时
    async fn relay_data(&self, mut client_stream: TcpStream, mut backend_stream: TcpStream) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client_addr = client_stream.peer_addr().ok();
        let backend_addr = backend_stream.peer_addr().ok();

        debug!("开始数据转发: {:?} <-> {:?}", client_addr, backend_addr);

        let result = tokio::io::copy_bidirectional(&mut client_stream, &mut backend_stream).await;

        match result {
            Ok((client_to_backend, backend_to_client)) => {
                debug!(
                    "数据转发完成: {:?} <-> {:?}, 客户端->后端: {} bytes, 后端->客户端: {} bytes",
                    client_addr, backend_addr, client_to_backend, backend_to_client
                );
            }
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        info!("对端关闭了连接: {:?} <-> {:?}", client_addr, backend_addr);
                    }
                    std::io::ErrorKind::ConnectionReset => {
                        info!("连接被重置: {:?} <-> {:?}", client_addr, backend_addr);
                    }
                    std::io::ErrorKind::ConnectionAborted => {
                        info!("连接被中止: {:?} <-> {:?}", client_addr, backend_addr);
                    }
                    std::io::ErrorKind::TimedOut => {
                        info!("连接因空闲超时而关闭: {:?} <-> {:?}", client_addr, backend_addr);
                    }
                    std::io::ErrorKind::ConnectionRefused => {
                        warn!("连接被拒绝: {:?} <-> {:?}", client_addr, backend_addr);
                    }
                    std::io::ErrorKind::HostUnreachable | std::io::ErrorKind::NetworkUnreachable => {
                        warn!("主机或网络不可达: {:?} <-> {:?}", client_addr, backend_addr);
                    }
                    _ => {
                        warn!("连接意外关闭: {:?} <-> {:?}, 错误: {}", client_addr, backend_addr, e);
                    }
                }
            }
        }

        // 确保连接正确关闭
        let _ = client_stream.shutdown().await;
        let _ = backend_stream.shutdown().await;

        Ok(())
    }
}

impl Clone for ProxyHandler {
    fn clone(&self) -> Self {
        Self {
            load_balancer: Arc::clone(&self.load_balancer),
            enable_proxy_protocol: self.enable_proxy_protocol,
            connect_timeout: self.connect_timeout,
            idle_timeout: self.idle_timeout,
            tcp_listener: Arc::clone(&self.tcp_listener),
            bind_addr: self.bind_addr,
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