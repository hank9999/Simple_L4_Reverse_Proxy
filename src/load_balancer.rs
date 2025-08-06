use crate::config::{BackendConfig, LoadBalanceStrategy};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::warn;

/// 加权轮询的后端节点
#[derive(Clone)]
struct BackendWeight {
    current_weight: i32,
    effective_weight: i32,
}

pub struct LoadBalancer {
    backends: Vec<BackendConfig>,
    strategy: LoadBalanceStrategy,
    current: AtomicUsize,
    // 加权轮询状态
    backend_weights: Arc<Mutex<Vec<BackendWeight>>>
}

impl LoadBalancer {
    pub fn new(backends: Vec<BackendConfig>, strategy: LoadBalanceStrategy) -> Self {
        if backends.is_empty() {
            // 这里需要直接退出程序, 让用户修改配置
            panic!("后端列表不能为空");
        }

        if backends.iter().any(|b| b.weight <= 0) {
            panic!("所有后端权重必须大于0");
        }

        // 初始化加权轮询后端列表
        let weighted_backends = backends
            .iter()
            .map(|b| BackendWeight {
                current_weight: 0,
                effective_weight: b.weight as i32,
            })
            .collect();

        Self {
            backends,
            strategy,
            current: AtomicUsize::new(0),
            backend_weights: Arc::new(Mutex::new(weighted_backends))
        }
    }

    pub fn select_backend(&self) -> Option<&BackendConfig> {
        // 优化：如果只有一个后端，直接返回
        if self.backends.len() == 1 {
            return Some(&self.backends[0]);
        }

        match self.strategy {
            LoadBalanceStrategy::RoundRobin => self.select_round_robin(),
            LoadBalanceStrategy::WeightedRoundRobin => self.select_weighted_round_robin(),
            // 最少连接数未完成 指向轮训
            LoadBalanceStrategy::LeastConnections => self.select_round_robin(),
        }
    }

    /// 简单轮询
    fn select_round_robin(&self) -> Option<&BackendConfig> {
        // 此处有溢出风险 可能会在极长时间后导致某次取值重复一次 影响几乎可以忽略
        // 为了简单与速度, 不修改
        let index = self.current.fetch_add(1, Ordering::Relaxed) % self.backends.len();
        Some(&self.backends[index])
    }

    /// 平滑加权轮询算法 (Smooth Weighted Round-Robin)
    /// 该算法能够平滑地分配请求，避免突发
    fn select_weighted_round_robin(&self) -> Option<&BackendConfig> {
        let mut weighted_backends = match self.backend_weights.lock() {
            Ok(guard) => guard,
            Err(_) => {
                // 降级到简单轮询
                warn!("加权轮询锁被污染，降级到简单轮询");
                return self.select_round_robin();
            }
        };
        if weighted_backends.is_empty() {
            return None;
        }

        let mut total_weight = 0;
        let mut selected_index = 0;
        let mut max_current_weight = i32::MIN;

        // 步骤1: 增加所有后端的当前权重
        // 步骤2: 选择当前权重最大的后端
        for (index, backend) in weighted_backends.iter_mut().enumerate() {
            backend.current_weight += backend.effective_weight;
            total_weight += backend.effective_weight;

            if backend.current_weight > max_current_weight {
                max_current_weight = backend.current_weight;
                selected_index = index;
            }
        }

        // 步骤3: 更新选中后端的权重并返回其配置
        weighted_backends[selected_index].current_weight -= total_weight;
        Some(&self.backends[selected_index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn create_test_backends() -> Vec<BackendConfig> {
        vec![
            BackendConfig {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001),
                weight: 1,
            },
            BackendConfig {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8002),
                weight: 2,
            },
            BackendConfig {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8003),
                weight: 3,
            },
        ]
    }

    #[test]
    fn test_single_backend_optimization() {
        let backends = vec![BackendConfig {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001),
            weight: 1,
        }];

        let lb = LoadBalancer::new(backends.clone(), LoadBalanceStrategy::RoundRobin);

        // 应该总是返回同一个后端
        for _ in 0..10 {
            let backend = lb.select_backend().unwrap();
            assert_eq!(backend.address.port(), 8001);
        }
    }

    #[test]
    fn test_round_robin() {
        let backends = create_test_backends();
        let lb = LoadBalancer::new(backends, LoadBalanceStrategy::RoundRobin);

        let mut selections = vec![];
        for _ in 0..6 {
            let backend = lb.select_backend().unwrap();
            selections.push(backend.address.port());
        }

        // 应该是均匀分布: 8001, 8002, 8003, 8001, 8002, 8003
        assert_eq!(selections, vec![8001, 8002, 8003, 8001, 8002, 8003]);
    }

    #[test]
    fn test_weighted_round_robin() {
        let backends = create_test_backends();
        let lb = LoadBalancer::new(backends, LoadBalanceStrategy::WeightedRoundRobin);

        let mut count_map = std::collections::HashMap::new();

        // 运行60次，统计分布
        for _ in 0..60 {
            let backend = lb.select_backend().unwrap();
            *count_map.entry(backend.address.port()).or_insert(0) += 1;
        }

        // 权重比例是 1:2:3，所以在60次中应该大约是 10:20:30
        assert_eq!(count_map.get(&8001), Some(&10));
        assert_eq!(count_map.get(&8002), Some(&20));
        assert_eq!(count_map.get(&8003), Some(&30));
    }
}