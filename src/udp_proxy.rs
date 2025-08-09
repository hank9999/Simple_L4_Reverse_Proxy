use crate::config::{BackendConfig, LoadBalanceStrategy, UdpConfig};
use crate::protocol::{Protocol, ProxyHeader, ProxyProtocolV2};
use anyhow::Result;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::time::{interval, timeout};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use crate::load_balancer::LoadBalancer;

#[derive(Debug)]
struct UdpSession {
    backend_addr: SocketAddr,
    last_activity: Instant,
    backend_socket: Arc<UdpSocket>,
    task_handle: JoinHandle<()>,
    cancel_token: CancellationToken,
}

pub struct UdpProxyHandler {
    sessions: Arc<DashMap<SocketAddr, UdpSession>>,
    load_balancer: Arc<LoadBalancer>,
    enable_proxy_protocol: bool,
    config: UdpConfig,
    client_socket: Arc<UdpSocket>,
}

impl UdpProxyHandler {
    pub async fn new(
        bind_addr: SocketAddr,
        backends: Vec<BackendConfig>,
        strategy: LoadBalanceStrategy,
        enable_proxy_protocol: bool,
        config: UdpConfig,
    ) -> Result<Self> {
        let client_socket = UdpSocket::bind(bind_addr).await?;
        info!("UDP 代理服务器启动，监听地址: {}", bind_addr);

        Ok(Self {
            sessions: Arc::new(DashMap::new()),
            load_balancer: Arc::new(LoadBalancer::new(backends, strategy)),
            enable_proxy_protocol,
            config,
            client_socket: Arc::new(client_socket),
        })
    }

    pub async fn run(&self) -> Result<()> {
        // 启动会话清理任务
        let sessions_cleanup = Arc::clone(&self.sessions);
        let session_timeout = Duration::from_secs(self.config.session_timeout);
        tokio::spawn(async move {
            let mut cleanup_interval = interval(Duration::from_secs(60));
            loop {
                cleanup_interval.tick().await;
                let now = Instant::now();

                let expired_sessions: Vec<_> = sessions_cleanup
                    .iter()
                    .filter(|entry| {
                        now.duration_since(entry.value().last_activity) >= session_timeout
                    })
                    .map(|entry| *entry.key())
                    .collect();

                for client_addr in expired_sessions {
                    if let Some((_, session)) = sessions_cleanup.remove(&client_addr) {
                        debug!("清理超时的 UDP 会话: {} -> {}", client_addr, session.backend_addr);

                        session.cancel_token.cancel();

                        if let Err(e) = session.task_handle.await {
                            error!("等待会话任务结束失败 {}: {}", client_addr, e);
                        } else {
                            debug!("会话任务已安全结束: {}", client_addr);
                        }
                    }
                }

                debug!("UDP 会话清理完成，当前会话数: {}", sessions_cleanup.len());
            }
        });

        // 主循环处理客户端数据包
        let mut buffer = vec![0u8; self.config.buffer_size];
        loop {
            match self.client_socket.recv_from(&mut buffer).await {
                Ok((len, client_addr)) => {
                    let data = buffer[..len].to_vec();
                    let handler = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handler.handle_packet(client_addr, data).await {
                            error!("处理 UDP 数据包失败: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("接收 UDP 数据包失败: {}", e);
                }
            }
        }
    }

    async fn handle_packet(&self, client_addr: SocketAddr, data: Vec<u8>) -> Result<()> {
        // 检查会话是否存在
        if let Some(mut session) = self.sessions.get_mut(&client_addr) {
            // --- 会话已存在，直接转发数据 ---
            session.last_activity = Instant::now();
            let backend_socket = Arc::clone(&session.backend_socket);
            let backend_addr = session.backend_addr;

            if let Err(e) = backend_socket.send(&data).await {
                error!("发送数据到后端服务器失败 (会话 {} -> {}): {}", client_addr, backend_addr, e);

                if let Some((_, session)) = self.sessions.remove(&client_addr) {
                    session.cancel_token.cancel();
                    tokio::spawn(async move {
                        if let Err(e) = session.task_handle.await {
                            error!("等待失败会话任务结束失败 {}: {}", client_addr, e);
                        }
                    });
                }
            } else {
                debug!("UDP 数据包转发 (已有会话): {} -> {} ({} bytes)", client_addr, backend_addr, data.len());
            }
        } else {
            // --- 会话不存在，创建新会话 ---
            if self.sessions.len() >= self.config.max_sessions {
                warn!("达到最大会话数限制: {}，丢弃来自 {} 的新连接", self.config.max_sessions, client_addr);
                return Ok(());
            }

            // 选择后端
            let backend = self.load_balancer.select_backend()
                .ok_or_else(|| anyhow::anyhow!("没有可用的后端服务器"))?;

            // 创建到后端的 UDP socket
            let backend_socket = UdpSocket::bind("0.0.0.0:0").await?;
            backend_socket.connect(backend.address).await?;
            let backend_socket = Arc::new(backend_socket);

            // --- 核心逻辑：处理第一个数据包 ---
            if self.enable_proxy_protocol {
                // 如果启用了 PROXY Protocol，将头部和数据一起发送
                let proxy_header = ProxyHeader {
                    src_addr: client_addr,
                    dst_addr: backend.address,
                    protocol: Protocol::UDP,
                };
                let header_bytes = ProxyProtocolV2::generate_header(&proxy_header);
                let full_packet = [header_bytes.to_vec(), data].concat();

                if let Err(e) = backend_socket.send(&full_packet).await {
                    error!("发送带 PROXY Protocol 头的数据到后端 {} 失败: {}", backend.address, e);
                    return Ok(());
                }
                debug!("UDP 数据包转发 (新会话, 含 PROXY Header): {} -> {} ({} bytes)", client_addr, backend.address, full_packet.len());
            } else {
                // 未启用 PROXY Protocol，直接发送原始数据
                if let Err(e) = backend_socket.send(&data).await {
                    error!("发送数据到后端 {} 失败: {}", backend.address, e);
                    return Ok(());
                }
                debug!("UDP 数据包转发 (新会话): {} -> {} ({} bytes)", client_addr, backend.address, data.len());
            }

            // --- 设置新会话并启动后端响应监听 ---
            self.setup_new_session(client_addr, backend.address, backend_socket);
        }

        Ok(())
    }

    fn setup_new_session(&self, client_addr: SocketAddr, backend_addr: SocketAddr, backend_socket: Arc<UdpSocket>) {
        let client_socket = Arc::clone(&self.client_socket);
        let sessions = Arc::clone(&self.sessions);
        let session_timeout = Duration::from_secs(self.config.session_timeout);
        let buffer_size = self.config.buffer_size;

        let cancel_token = CancellationToken::new();
        let cancel_token_task = cancel_token.clone();
        let backend_socket_task = Arc::clone(&backend_socket);

        let task_handle = tokio::spawn(async move {
            let mut buffer = vec![0u8; buffer_size];
            loop {
                tokio::select! {
                    result = timeout(session_timeout, backend_socket_task.recv(&mut buffer)) => {
                        match result {
                            Ok(Ok(len)) => {
                                let data = &buffer[..len];
                                if let Err(e) = client_socket.send_to(data, client_addr).await {
                                    error!("发送响应到客户端 {} 失败: {}", client_addr, e);
                                    break;
                                }

                                if let Some(mut s) = sessions.get_mut(&client_addr) {
                                    s.last_activity = Instant::now();
                                }

                                debug!("UDP 响应转发: {} -> {} ({} bytes)", backend_addr, client_addr, len);
                            }
                            Ok(Err(e)) => {
                                error!("从后端服务器 {} 接收数据失败: {}", backend_addr, e);
                                break;
                            }
                            Err(_) => {
                                debug!("后端服务器 {} 响应超时，关闭会话 for {}", backend_addr, client_addr);
                                break;
                            }
                        }
                    }
                    _ = cancel_token_task.cancelled() => {
                        debug!("UDP 会话任务被取消: {} <-> {}", client_addr, backend_addr);
                        break;
                    }
                }
            }

            if sessions.remove(&client_addr).is_some() {
                info!("UDP 会话关闭: {} <-> {}", client_addr, backend_addr);
            }
        });

        let session = UdpSession {
            backend_addr,
            last_activity: Instant::now(),
            backend_socket,
            task_handle,
            cancel_token,
        };

        self.sessions.insert(client_addr, session);
        info!("创建新的 UDP 会话: {} -> {} (总会话数: {})", client_addr, backend_addr, self.sessions.len());
    }
}

impl Clone for UdpProxyHandler {
    fn clone(&self) -> Self {
        Self {
            sessions: Arc::clone(&self.sessions),
            load_balancer: Arc::clone(&self.load_balancer),
            enable_proxy_protocol: self.enable_proxy_protocol,
            config: self.config.clone(),
            client_socket: Arc::clone(&self.client_socket),
        }
    }
}