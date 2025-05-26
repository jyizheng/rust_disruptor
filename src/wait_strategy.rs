// src/wait_strategy.rs

use crate::sequencer::Sequence;
use std::sync::{Arc, Mutex, Condvar}; // 引入 Condvar 和 Mutex
use std::thread; // 用于 thread::yield_now 或 Condvar 唤醒
use std::hint; // For `spin_loop_hint`
use std::time::Duration; // 用于 Condvar 的 wait_timeout_with_timeout_or_until

/// Trait defining the interface for different wait strategies.
/// 
/// 实现了 Send + Sync + 'static + Clone + Default trait。
/// - `Send` 和 `Sync` 允许在线程间安全共享。
/// - `'static` 表示没有引用外部生命周期。
/// - `Clone` 允许 Disruptor 在内部复制策略实例。
/// - `Default` 允许在 Disruptor.new() 中默认创建策略实例。
pub trait WaitStrategy: Send + Sync + 'static + Clone + Default { // <-- 添加 Clone 和 Default
    /// Waits until the `consumer_sequence` is less than or equal to the `gating_sequence`.
    ///
    /// This method is called by a consumer to wait for an event at a particular sequence
    /// to become available (i.e., published by the producer and potentially
    /// processed by upstream consumers if there are dependencies).
    ///
    /// # Arguments
    /// * `sequence`: The sequence number the consumer is trying to reach.
    /// * `cursor`: The producer's current published sequence (global cursor).
    /// * `dependent_sequence`: (Optional) The sequence of a dependent consumer (if any),
    ///   meaning we also need to wait for this consumer to pass the `sequence`.
    /// * `consumer_sequence`: The consumer's own current sequence. (Not directly used for blocking condition in all strategies)
    ///
    /// # Returns
    /// The actual highest sequence number that has become available and meets dependencies.
    fn wait_for(
        &self,
        sequence: i64,
        cursor: Arc<Sequence>,
        dependent_sequence: Option<Arc<Sequence>>,
        consumer_sequence: Arc<Sequence>, // Still part of trait signature, though not used in BlockingWaitStrategy
    ) -> i64;

    /// Signals all waiting consumers that new events might be available.
    /// This method is called by the `Sequencer` after an event is published.
    fn signal_all(&self); // <-- 新增方法
}

/// A `WaitStrategy` that continuously busy-spins until the event is available.
///
/// This strategy offers the lowest latency in low-contention scenarios but
/// consumes 100% CPU when waiting. It's generally not recommended for
/// high-contention or low-frequency scenarios in production.
#[derive(Clone, Default)] // <-- 添加 Default
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
                        return available_sequence; // 返回实际可用序列号
                    }
                } else {
                    return available_sequence; // 返回实际可用序列号
                }
            }

            hint::spin_loop();
        }
    }

    fn signal_all(&self) {
        // 对于忙自旋策略，通常不需要显式信号，因为消费者一直在检查。
        // 但为了遵循 trait，我们提供一个空实现。
    }
}


/// A `WaitStrategy` that uses a `Condvar` to block consumer threads when no events are available.
/// This strategy offers the lowest CPU usage but introduces higher latency due to context switching.
#[derive(Clone)] // Condvar/Mutex need to be cloned via Arc
pub struct BlockingWaitStrategy {
    // Arc<(Mutex<bool>, Condvar)> 是一个元组，包含一个 Mutex 和一个 Condvar。
    // bool 值用于 Condvar 的谓词（predicate）， Condvar 用于等待和唤醒。
    // Arc 允许多个线程共享 Condvar 实例。
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
        let (lock, cvar) = &*self.notification_pair; // 解引用 Arc，获取元组的引用
        let mut ready_flag = lock.lock().unwrap(); // 获取 Mutex 锁

        loop {
            // 检查条件：生产者光标是否达到或超过目标序列，以及依赖项是否满足。
            let available_sequence = cursor.get();
            let dependencies_met = if let Some(dep_seq) = &dependent_sequence {
                dep_seq.get() >= sequence
            } else {
                true
            };

            if available_sequence >= sequence && dependencies_met {
                *ready_flag = false; // 条件满足，重置通知标志
                return available_sequence; // 返回实际可用序列号
            }

            // 条件不满足，线程进入等待状态。
            // `wait` 会原子性地释放锁并阻塞当前线程，直到被唤醒。
            // 唤醒后，它会重新获取锁。
            ready_flag = cvar.wait(ready_flag).unwrap();
        }
    }

    fn signal_all(&self) {
        let (lock, cvar) = &*self.notification_pair;
        let mut ready_flag = lock.lock().unwrap();
        *ready_flag = true; // 设置通知标志
        cvar.notify_all(); // 唤醒所有等待在该 Condvar 上的线程
    }
}
