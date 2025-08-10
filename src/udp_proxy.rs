use anyhow::Result;
use bytes::{Bytes, BytesMut};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, Semaphore, OwnedSemaphorePermit};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use crate::config::{BackendConfig, LoadBalanceStrategy, UdpConfig};
use crate::load_balancer::LoadBalancer;
use crate::protocol::{Protocol, ProxyHeader, ProxyProtocolV2};

const BUFFER_POOL_SIZE: usize = 1024;
const MAX_PACKET_SIZE: usize = 65535;

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
        // 使用 try_recv 是合理的，因为我们不想等待
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
        if buffer.capacity() <= self.buffer_size * 2 { // 避免缓冲区过度增长
            buffer.clear();
            // try_send 失败也无所谓，只是这个 buffer 被丢弃了而已
            let _ = self.pool.try_send(buffer);
        }
    }
}

/// UDP会话状态。
#[derive(Debug)]
struct UdpSession {
    backend_socket: Arc<UdpSocket>,
    last_activity_ms: AtomicU64,
    cancel_token: CancellationToken,
}

impl UdpSession {
    /// 更新活动时间戳
    fn update_activity(&self, epoch: Instant) {
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
    session_count: Arc<AtomicUsize>,
    session_semaphore: Arc<Semaphore>,
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
        info!("UDP 代理服务器启动，监听地址: {}", bind_addr);

        let buffer_size = config.buffer_size.min(MAX_PACKET_SIZE);
        let buffer_pool = Arc::new(BufferPool::new(BUFFER_POOL_SIZE, buffer_size));
        let max_sessions = config.max_sessions;

        Ok(Self {
            sessions: Arc::new(DashMap::new()),
            load_balancer: Arc::new(LoadBalancer::new(backends, strategy)),
            enable_proxy_protocol,
            config,
            client_socket: Arc::new(client_socket),
            buffer_pool,
            session_count: Arc::new(AtomicUsize::new(0)),
            session_semaphore: Arc::new(Semaphore::new(max_sessions)),
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
                                    error!("处理UDP数据包失败: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("接收UDP数据包失败: {}", e);
                            self.buffer_pool.release(buffer);
                        }
                    }
                }
                _ = self.shutdown_token.cancelled() => {
                    info!("UDP代理服务器正在关闭...");
                    break;
                }
            }
        }
        Ok(())
    }

    fn start_cleanup_task(&self) {
        let sessions = Arc::clone(&self.sessions);
        let session_timeout = Duration::from_secs(self.config.session_timeout);
        let shutdown_token = self.shutdown_token.clone();
        let epoch = self.epoch;

        tokio::spawn(async move {
            let mut cleanup_interval = interval(Duration::from_secs(30));
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
                                // 只需取消任务即可，后台任务会自动清理信号量和计数器
                                session.cancel_token.cancel();
                                debug!("通过清理任务关闭超时UDP会话: {}", client_addr);
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
        // 1. 在操作 DashMap 之前，先获取许可。如果达到上限，这里会异步等待。
        let permit = self.session_semaphore.clone().acquire_owned().await?;

        // 2. 使用 Entry API 进行原子性检查和插入
        match self.sessions.entry(client_addr) {
            Entry::Occupied(entry) => {
                // 获取许可后，转发数据包即可，获得的许可会自动归还。
                let session = entry.get();
                session.update_activity(self.epoch);
                session.backend_socket.send(&data).await?;
            }
            Entry::Vacant(entry) => {
                // 创新新会话
                if let Err(e) = self.build_and_launch_session(entry, permit, data).await {
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
        permit: OwnedSemaphorePermit,
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
            let mut full_packet = BytesMut::with_capacity(header_bytes.len() + first_packet_data.len());
            full_packet.extend_from_slice(&header_bytes);
            full_packet.extend_from_slice(&first_packet_data);
            backend_socket.send(&full_packet).await?;
        } else {
            backend_socket.send(&first_packet_data).await?;
        }

        // 创建会话
        let cancel_token = CancellationToken::new();
        let new_session = Arc::new(UdpSession {
            backend_socket: Arc::clone(&backend_socket),
            last_activity_ms: AtomicU64::new(self.epoch.elapsed().as_millis() as u64),
            cancel_token: cancel_token.clone(),
        });

        // 启动后端响应处理任务
        self.start_backend_handler(entry.key(), backend.address, backend_socket, cancel_token);

        let key = entry.key().clone();

        // 原子性地插入会话
        entry.insert(new_session);
        self.session_count.fetch_add(1, Ordering::Relaxed);

        // 只有在所有操作成功并插入会话后，才消耗许可，防止其被归还
        std::mem::forget(permit);

        info!("创建新UDP会话: {} -> {} (总会话数: {})", key, backend.address, self.session_count.load(Ordering::Relaxed));
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
        let session_count = Arc::clone(&self.session_count);
        let session_semaphore = Arc::clone(&self.session_semaphore);
        let epoch = self.epoch;
        let client_addr = *client_addr;

        tokio::spawn(async move {
            loop {
                let mut buffer = buffer_pool.acquire().await;
                buffer.resize(MAX_PACKET_SIZE, 0);

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

            // 任务结束后的统一清理逻辑
            if sessions.remove(&client_addr).is_some() {
                session_count.fetch_sub(1, Ordering::Relaxed);
                session_semaphore.add_permits(1); // 归还许可
                info!("UDP会话关闭: {} <-> {} (剩余会话数: {})", client_addr, backend_addr, session_count.load(Ordering::Relaxed));
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
            session_count: Arc::clone(&self.session_count),
            session_semaphore: Arc::clone(&self.session_semaphore),
            shutdown_token: self.shutdown_token.clone(),
            epoch: self.epoch,
        }
    }
}