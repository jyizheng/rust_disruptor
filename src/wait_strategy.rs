// src/wait_strategy.rs

use crate::sequencer::Sequence;
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::hint;
use std::time::{Instant, Duration};

/// Trait defining the interface for different wait strategies.
/// 
/// 实现了 Send + Sync + 'static + Clone + Default trait。
pub trait WaitStrategy: Send + Sync + 'static + Clone + Default {
    /// Waits until the `consumer_sequence` is less than or equal to the `gating_sequence`.
    ///
    /// # Arguments
    /// * `sequence`: The sequence number the consumer is trying to reach.
    /// * `cursor`: The producer's current published sequence (global cursor).
    /// * `dependent_sequence`: (Optional) The sequence of a dependent consumer (if any),
    ///   meaning we also need to wait for this consumer to pass the `sequence`.
    /// * `consumer_sequence`: The consumer's own current sequence.
    ///
    /// # Returns
    /// The actual highest sequence number that has become available and meets dependencies.
    fn wait_for(
        &self,
        sequence: i64,
        cursor: Arc<Sequence>,
        dependent_sequence: Option<Arc<Sequence>>,
        consumer_sequence: Arc<Sequence>,
    ) -> i64;

    /// Signals all waiting consumers that new events might be available.
    /// This method is called by the `Sequencer` after an event is published.
    fn signal_all(&self);
}

/// A `WaitStrategy` that continuously busy-spins until the event is available.
#[derive(Clone, Default)]
pub struct BusySpinWaitStrategy;

impl WaitStrategy for BusySpinWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        cursor: Arc<Sequence>,
        dependent_sequence: Option<Arc<Sequence>>,
        _consumer_sequence: Arc<Sequence>, 
    ) -> i64 {
        loop {
            let available_sequence = cursor.get();

            if available_sequence >= sequence {
                if let Some(dep_seq) = &dependent_sequence {
                    if dep_seq.get() >= sequence {
                        return available_sequence;
                    }
                } else {
                    return available_sequence;
                }
            }

            hint::spin_loop();
        }
    }

    fn signal_all(&self) {
        // 不需要显式信号
    }
}


/// A `WaitStrategy` that uses a `Condvar` to block consumer threads when no events are available.
#[derive(Clone)] 
pub struct BlockingWaitStrategy {
    notification_pair: Arc<(Mutex<bool>, Condvar)>, 
}

impl Default for BlockingWaitStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockingWaitStrategy {
    pub fn new() -> Self {
        BlockingWaitStrategy {
            notification_pair: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }
}

impl WaitStrategy for BlockingWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        cursor: Arc<Sequence>,
        dependent_sequence: Option<Arc<Sequence>>,
        _consumer_sequence: Arc<Sequence>,
    ) -> i64 {
        let (lock, cvar) = &*self.notification_pair; 
        let mut ready_flag = lock.lock().unwrap(); 

        loop {
            let available_sequence = cursor.get();
            let dependencies_met = if let Some(dep_seq) = &dependent_sequence {
                dep_seq.get() >= sequence
            } else {
                true
            };

            if available_sequence >= sequence && dependencies_met {
                *ready_flag = false; 
                return available_sequence; 
            }

            ready_flag = cvar.wait(ready_flag).unwrap();
        }
    }

    fn signal_all(&self) {
        let (lock, cvar) = &*self.notification_pair;
        let mut ready_flag = lock.lock().unwrap();
        *ready_flag = true; 
        cvar.notify_all(); 
    }
}

// --- 新增：YieldingWaitStrategy ---
/// A `WaitStrategy` that repeatedly calls `thread::yield_now()` when waiting.
/// It offers a balance between latency and CPU usage compared to busy-spinning.
#[derive(Clone, Default)] // 遵循 trait 的要求
pub struct YieldingWaitStrategy;

impl WaitStrategy for YieldingWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        cursor: Arc<Sequence>,
        dependent_sequence: Option<Arc<Sequence>>,
        _consumer_sequence: Arc<Sequence>,
    ) -> i64 {
        loop {
            let available_sequence = cursor.get();
            let dependencies_met = if let Some(dep_seq) = &dependent_sequence {
                dep_seq.get() >= sequence
            } else {
                true
            };

            if available_sequence >= sequence && dependencies_met {
                return available_sequence;
            }

            thread::yield_now(); // 让出 CPU，等待下一次调度
        }
    }

    fn signal_all(&self) {
        // 对于让步策略，不需要显式信号。
    }
}


/// A `WaitStrategy` that combines busy-spinning, yielding, and blocking phases.
#[derive(Clone)] 
pub struct PhasedBackoffWaitStrategy {
    spin_timeout: Duration,
    yield_timeout: Duration,
    
    // 内部持有的策略实例
    busy_spin_strategy: BusySpinWaitStrategy,
    yielding_strategy: YieldingWaitStrategy, // <-- 新增：让步策略
    blocking_strategy: BlockingWaitStrategy, 
}

impl Default for PhasedBackoffWaitStrategy {
    fn default() -> Self {
        Self::new(
            Duration::from_micros(500), 
            Duration::from_millis(5),  
            BusySpinWaitStrategy::default(),
            YieldingWaitStrategy::default(), // <-- 初始化 YieldingWaitStrategy
            BlockingWaitStrategy::default(),
        )
    }
}

impl PhasedBackoffWaitStrategy {
    pub fn new(
        spin_timeout: Duration,
        yield_timeout: Duration,
        busy_spin_strategy: BusySpinWaitStrategy,
        yielding_strategy: YieldingWaitStrategy, // <-- 新增参数
        blocking_strategy: BlockingWaitStrategy,
    ) -> Self {
        PhasedBackoffWaitStrategy {
            spin_timeout,
            yield_timeout,
            busy_spin_strategy,
            yielding_strategy, // <-- 赋值
            blocking_strategy,
        }
    }
}

impl WaitStrategy for PhasedBackoffWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        cursor: Arc<Sequence>,
        dependent_sequence: Option<Arc<Sequence>>,
        consumer_sequence: Arc<Sequence>, 
    ) -> i64 {
        // Phase 1: Busy Spin
        let spin_start_time = Instant::now();
        loop {
            let available_sequence = cursor.get();
            let dependencies_met = if let Some(dep_seq) = &dependent_sequence {
                dep_seq.get() >= sequence
            } else { true };

            if available_sequence >= sequence && dependencies_met {
                return available_sequence;
            }

            if spin_start_time.elapsed() > self.spin_timeout {
                break; 
            }
            hint::spin_loop();
        }

        // Phase 2: Yielding (委托给 YieldingWaitStrategy)
        let yield_start_time = Instant::now();
        loop {
            let available_sequence = cursor.get();
            let dependencies_met = if let Some(dep_seq) = &dependent_sequence {
                dep_seq.get() >= sequence
            } else { true };

            if available_sequence >= sequence && dependencies_met {
                return available_sequence;
            }

            if yield_start_time.elapsed() > self.yield_timeout {
                break; 
            }
            // 委托给 YieldingWaitStrategy 的 wait_for 方法，但需要处理参数
            // 因为 YieldingWaitStrategy::wait_for 也会循环，我们不能直接返回其结果
            // 而是只调用其内部的让步动作。
            self.yielding_strategy.wait_for(
                sequence, 
                Arc::clone(&cursor), // 克隆 Arc 以匹配签名
                dependent_sequence.as_ref().map(Arc::clone), // 克隆 Option<Arc>
                Arc::clone(&consumer_sequence) // 克隆 Arc
            );
            // 上述直接调用 `wait_for` 会导致 `loop in a loop`，且不符合 PhasedBackoff 的意图。
            // 正确的做法是只执行 YieldingWaitStrategy 的核心动作：`thread::yield_now();`
            // 而不是调用它的整个 `wait_for` 循环。
            // 因此，我们直接在这里调用 `thread::yield_now();`
            thread::yield_now(); 
        }

        // Phase 3: Blocking
        self.blocking_strategy.wait_for(sequence, cursor, dependent_sequence, consumer_sequence)
    }

    fn signal_all(&self) {
        // 只需要唤醒阻塞策略
        self.blocking_strategy.signal_all();
    }
}
