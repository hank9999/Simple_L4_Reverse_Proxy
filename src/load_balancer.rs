use crate::config::{BackendConfig, LoadBalanceStrategy};
use std::sync::atomic::{AtomicUsize, Ordering};

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