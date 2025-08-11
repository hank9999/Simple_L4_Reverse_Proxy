use anyhow::Result;
use bytes::{Bytes, BytesMut};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use crate::config::{BackendConfig, LoadBalanceStrategy, UdpConfig};
use crate::load_balancer::LoadBalancer;
use crate::protocol::{Protocol, ProxyHeader, ProxyProtocolV2};

const BUFFER_POOL_SIZE: usize = 1024;
// UDP 实际最大载荷大小（65535 - 8字节UDP头 - 20字节IP头）
const MAX_PACKET_SIZE: usize = 65507;

/// 缓冲池，用于复用内存以减少分配开销。
struct BufferPool {
    pool: mpsc::Sender<BytesMut>,
    receiver: Arc<Mutex<mpsc::Receiver<BytesMut>>>,
    buffer_size: usize,
}

impl BufferPool {
    fn new(size: usize, buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(size);
        Self {
            pool: tx,
            receiver: Arc::new(Mutex::new(rx)),
            buffer_size,
        }
    }

    async fn acquire(&self) -> BytesMut {
        let mut rx = self.receiver.lock().await;
        // 如果池中没有可用缓冲区，立即创建新的
        match rx.try_recv() {
            Ok(mut buffer) => {
                buffer.clear(); // 确保缓冲区是干净的
                buffer
            },
            Err(_) => BytesMut::with_capacity(self.buffer_size),
        }
    }

    fn release(&self, mut buffer: BytesMut) {
        // 只回收容量符合缓冲区（允许1KB的余量）
        if buffer.capacity() <= self.buffer_size + 1024 {
            buffer.clear();
            // try_send 失败也无所谓，只是这个 buffer 被丢弃了而已
            let _ = self.pool.try_send(buffer);
        }
    }
}

/// UDP会话状态。
struct UdpSession {
    backend_socket: Arc<UdpSocket>,
    last_activity_ms: AtomicU64,
    cancel_token: CancellationToken,
    // SessionGuard，用于确保会话结束时自动减少计数
    _guard: Option<SessionGuard>,
}

impl UdpSession {
    /// 更新活动时间戳
    fn update_activity(&self, epoch: Instant) {
        // 截断没有问题 2^64-1毫秒 ==> 5.8亿年
        let elapsed_ms = epoch.elapsed().as_millis() as u64;
        self.last_activity_ms.store(elapsed_ms, Ordering::Relaxed);
    }

    /// 获取上次活动的 Instant
    fn get_last_activity(&self, epoch: Instant) -> Instant {
        let elapsed_ms = self.last_activity_ms.load(Ordering::Relaxed);
        epoch + Duration::from_millis(elapsed_ms)
    }
}

/// UDP代理处理器
pub struct UdpProxyHandler {
    sessions: Arc<DashMap<SocketAddr, Arc<UdpSession>>>,
    load_balancer: Arc<LoadBalancer>,
    enable_proxy_protocol: bool,
    config: UdpConfig,
    client_socket: Arc<UdpSocket>,
    buffer_pool: Arc<BufferPool>,
    // 活跃会话计数器
    active_sessions: Arc<AtomicUsize>,
    // 最大会话数，0 表示不限制
    max_sessions: usize,
    shutdown_token: CancellationToken,
    // 启动时间戳
    epoch: Instant,
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

        let buffer_size = config.buffer_size.min(MAX_PACKET_SIZE);
        let buffer_pool = Arc::new(BufferPool::new(BUFFER_POOL_SIZE, buffer_size));
        let max_sessions = config.max_sessions;

        // 根据 max_sessions 的值显示不同的日志信息
        if max_sessions == 0 {
            info!("UDP 代理服务器启动，监听地址: {}, 会话数: 无限制", bind_addr);
        } else {
            info!("UDP 代理服务器启动，监听地址: {}, 最大会话数: {}", bind_addr, max_sessions);
        }

        Ok(Self {
            sessions: Arc::new(DashMap::new()),
            load_balancer: Arc::new(LoadBalancer::new(backends, strategy)),
            enable_proxy_protocol,
            config,
            client_socket: Arc::new(client_socket),
            buffer_pool,
            active_sessions: Arc::new(AtomicUsize::new(0)),
            max_sessions,
            shutdown_token: CancellationToken::new(),
            epoch: Instant::now(),
        })
    }

    pub async fn run(&self) -> Result<()> {
        self.start_cleanup_task();

        loop {
            let mut buffer = self.buffer_pool.acquire().await;
            buffer.resize(self.config.buffer_size, 0);

            tokio::select! {
                result = self.client_socket.recv_from(&mut buffer) => {
                    match result {
                        Ok((len, client_addr)) => {
                            let data = buffer.split_to(len).freeze();
                            self.buffer_pool.release(buffer); // 立即释放 buffer

                            let handler = self.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handler.handle_packet(client_addr, data).await {
                                    error!("处理 UDP 数据包失败: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("接收 UDP 数据包失败: {}", e);
                            self.buffer_pool.release(buffer);
                        }
                    }
                }
                _ = self.shutdown_token.cancelled() => {
                    info!("UDP 代理服务器正在关闭...");
                    break;
                }
            }
        }
        Ok(())
    }

    fn start_cleanup_task(&self) {
        let sessions = Arc::clone(&self.sessions);
        let session_timeout = Duration::from_secs(self.config.session_timeout);
        let cleanup_interval = Duration::from_secs(self.config.cleanup_interval);
        let shutdown_token = self.shutdown_token.clone();
        let epoch = self.epoch;

        tokio::spawn(async move {
            let mut cleanup_interval = interval(cleanup_interval);
            loop {
                tokio::select! {
                    _ = cleanup_interval.tick() => {
                        let now = Instant::now();
                        let expired_keys: Vec<_> = sessions.iter()
                            .filter(|entry| {
                                let last_activity = entry.value().get_last_activity(epoch);
                                now.duration_since(last_activity) >= session_timeout
                            })
                            .map(|entry| *entry.key())
                            .collect();

                        for client_addr in expired_keys {
                            if let Some((_, session)) = sessions.remove(&client_addr) {
                                // 取消后台任务，SessionGuard 会自动处理计数器
                                session.cancel_token.cancel();
                                debug!("通过清理任务关闭超时 UDP 会话: {}", client_addr);
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        info!("会话清理任务正在退出...");
                        sessions.iter().for_each(|entry| entry.value().cancel_token.cancel());
                        sessions.clear();
                        break;
                    }
                }
            }
        });
    }

    async fn handle_packet(&self, client_addr: SocketAddr, data: Bytes) -> Result<()> {
        // 尝试获取现有会话并转发（热路径）
        if let Some(entry) = self.sessions.get(&client_addr) {
            let session = entry.value();
            session.update_activity(self.epoch);
            if let Err(e) = session.backend_socket.send(&data).await {
                error!("转发数据到后端失败: {}", e);
                // 发送失败时，移除会话
                if let Some((_, session)) = self.sessions.remove(&client_addr) {
                    session.cancel_token.cancel();
                }
            }
            return Ok(());
        }

        // 如果会话不存在，进入创建流程（冷路径）
        self.create_session_and_send(client_addr, data).await
    }

    /// 原子性地创建会话并发送第一个数据包。
    async fn create_session_and_send(&self, client_addr: SocketAddr, data: Bytes) -> Result<()> {
        // 使用 Entry API 进行原子性检查和插入
        match self.sessions.entry(client_addr) {
            Entry::Occupied(entry) => {
                // 会话已存在，直接转发数据包
                let session = entry.get();
                session.update_activity(self.epoch);
                session.backend_socket.send(&data).await?;
            }
            Entry::Vacant(entry) => {
                // 检查是否达到最大会话数限制（只有当 max_sessions > 0 时才进行限制）
                if self.max_sessions > 0 {
                    let current_sessions = self.active_sessions.load(Ordering::Relaxed);
                    if current_sessions >= self.max_sessions {
                        warn!("UDP 达到最大会话数限制 ({}), 拒绝新会话 {}", self.max_sessions, client_addr);
                        return Ok(());
                    }
                }
                
                // 创建新会话
                if let Err(e) = self.build_and_launch_session(entry, data).await {
                    warn!("构建新会话失败 for {}: {}", client_addr, e);
                }
            }
        }
        Ok(())
    }

    /// 辅助函数，处理新会话的构建、启动和插入逻辑
    async fn build_and_launch_session<'a>(
        &self,
        entry: dashmap::mapref::entry::VacantEntry<'a, SocketAddr, Arc<UdpSession>>,
        first_packet_data: Bytes,
    ) -> Result<()> {
        // 选择后端
        let backend = self.load_balancer.select_backend()
            .ok_or_else(|| anyhow::anyhow!("没有可用的后端服务器"))?;

        // 创建到后端的UDP socket
        let backend_socket = UdpSocket::bind("0.0.0.0:0").await?;
        backend_socket.connect(backend.address).await?;
        let backend_socket = Arc::new(backend_socket);

        // 准备并发送第一个数据包（这是唯一可能发送 PROXY Protocol 头的地方）
        if self.enable_proxy_protocol {
            let proxy_header = ProxyHeader { src_addr: *entry.key(), dst_addr: backend.address, protocol: Protocol::UDP };
            let header_bytes = ProxyProtocolV2::generate_header(&proxy_header);
            
            // 检查附加协议头后是否超过 UDP 最大载荷
            let total_size = header_bytes.len() + first_packet_data.len();
            if total_size > MAX_PACKET_SIZE {
                warn!("附加协议头后超过 UDP 最大载荷限制: {} > {}", total_size, MAX_PACKET_SIZE);
                // 发送原始数据，不加头部
                backend_socket.send(&first_packet_data).await?;
            } else {
                let mut full_packet = BytesMut::with_capacity(total_size);
                full_packet.extend_from_slice(&header_bytes);
                full_packet.extend_from_slice(&first_packet_data);
                backend_socket.send(&full_packet).await?;
            }
        } else {
            backend_socket.send(&first_packet_data).await?;
        }

        // 创建会话守卫（如果启用了会话限制）
        let guard = if self.max_sessions > 0 {
            // 增加活跃会话计数
            self.active_sessions.fetch_add(1, Ordering::Relaxed);
            Some(SessionGuard::new(Arc::clone(&self.active_sessions)))
        } else {
            None
        };

        // 创建会话
        let cancel_token = CancellationToken::new();
        let new_session = Arc::new(UdpSession {
            backend_socket: Arc::clone(&backend_socket),
            last_activity_ms: AtomicU64::new(self.epoch.elapsed().as_millis() as u64),
            cancel_token: cancel_token.clone(),
            _guard: guard,
        });

        // 启动后端响应处理任务
        self.start_backend_handler(entry.key(), backend.address, backend_socket, cancel_token);

        let key = entry.key().clone();

        // 原子性地插入会话
        entry.insert(new_session);

        let current_sessions = self.active_sessions.load(Ordering::Relaxed);
        info!("创建新 UDP 会话: {} -> {} (总会话数: {})", key, backend.address, current_sessions);
        Ok(())
    }

    fn start_backend_handler(
        &self,
        client_addr: &SocketAddr,
        backend_addr: SocketAddr,
        backend_socket: Arc<UdpSocket>,
        cancel_token: CancellationToken,
    ) {
        let client_socket = Arc::clone(&self.client_socket);
        let sessions = Arc::clone(&self.sessions);
        let buffer_pool = Arc::clone(&self.buffer_pool);
        let session_timeout = Duration::from_secs(self.config.session_timeout);
        let active_sessions = Arc::clone(&self.active_sessions);
        let epoch = self.epoch;
        let client_addr = *client_addr;

        tokio::spawn(async move {
            loop {
                let mut buffer = buffer_pool.acquire().await;
                // 使用与主循环相同的缓冲区大小
                buffer.resize(buffer_pool.buffer_size.min(MAX_PACKET_SIZE), 0);

                tokio::select! {
                    result = tokio::time::timeout(session_timeout, backend_socket.recv(&mut buffer)) => {
                        match result {
                            Ok(Ok(len)) => {
                                // 成功从后端接收数据
                                if let Some(session) = sessions.get(&client_addr) {
                                    session.update_activity(epoch);
                                }
                                if client_socket.send_to(&buffer[..len], client_addr).await.is_err() {
                                    break; // 发送给客户端失败，关闭会话
                                }
                            }
                            Ok(Err(_)) => break, // 从后端接收失败，关闭会话
                            Err(_) => { // 接收超时
                                debug!("后端响应超时，关闭会话 for {}", client_addr);
                                break;
                            }
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        // 会话被外部（清理任务或失败）取消
                        break;
                    }
                }
            }

            // 任务结束后的清理逻辑
            if sessions.remove(&client_addr).is_some() {
                // SessionGuard 会在 drop 时自动减少计数器
                let remaining_sessions = active_sessions.load(Ordering::Relaxed);
                info!("UDP 会话关闭: {} <-> {} (剩余会话数: {})", client_addr, backend_addr, remaining_sessions);
            }
        });
    }

    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }
}

// Clone 实现需要保持所有 Arc 指针的同步
impl Clone for UdpProxyHandler {
    fn clone(&self) -> Self {
        Self {
            sessions: Arc::clone(&self.sessions),
            load_balancer: Arc::clone(&self.load_balancer),
            enable_proxy_protocol: self.enable_proxy_protocol,
            config: self.config.clone(),
            client_socket: Arc::clone(&self.client_socket),
            buffer_pool: Arc::clone(&self.buffer_pool),
            active_sessions: Arc::clone(&self.active_sessions),
            max_sessions: self.max_sessions,
            shutdown_token: self.shutdown_token.clone(),
            epoch: self.epoch,
        }
    }
}

/// RAII 结构，确保会话结束时自动减少计数
struct SessionGuard {
    counter: Arc<AtomicUsize>,
}

impl SessionGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}