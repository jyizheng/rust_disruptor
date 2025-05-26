// src/wait_strategy.rs

use crate::sequencer::Sequence;
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::hint;
use std::time::{Instant, Duration};

/// Trait defining the interface for different wait strategies.
pub trait WaitStrategy: Send + Sync + 'static {
    /// Waits until all `gating_sequences` are greater than or equal to the `sequence`.
    ///
    /// # Arguments
    /// * `sequence`: The target sequence number the consumer is trying to reach.
    /// * `gating_sequences`: A slice of `Arc<Sequence>` representing all sequences
    ///   that must be processed up to (or beyond) `sequence` before this consumer can proceed.
    ///   This typically includes the producer's cursor and any upstream consumer sequences.
    /// * `consumer_sequence`: The consumer's own current sequence.
    ///
    /// # Returns
    /// The actual highest sequence number that has become available and meets all gating conditions.
    fn wait_for(
        &self,
        sequence: i64,
        gating_sequences: &[Arc<Sequence>], // <-- 修改签名：现在是一个序列切片
        consumer_sequence: Arc<Sequence>,
    ) -> i64;

    /// Signals all waiting consumers that new events might be available.
    fn signal_all(&self);

    /// Clones the trait object into a `Box<dyn WaitStrategy>`.
    fn clone_box(&self) -> Box<dyn WaitStrategy>;
}

// --- 为 `Box<dyn WaitStrategy>` 实现 Clone trait ---
impl Clone for Box<dyn WaitStrategy> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}


/// A `WaitStrategy` that continuously busy-spins until the event is available.
#[derive(Clone, Default)]
pub struct BusySpinWaitStrategy;

impl WaitStrategy for BusySpinWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        gating_sequences: &[Arc<Sequence>], // <-- 修改签名
        _consumer_sequence: Arc<Sequence>, 
    ) -> i64 {
        loop {
            // 获取所有门控序列中的最小值
            let available_sequence = get_minimum_sequence(gating_sequences);

            if available_sequence >= sequence {
                return available_sequence; // 返回实际可用序列号
            }

            hint::spin_loop();
        }
    }

    fn signal_all(&self) {
        // 不需要显式信号
    }

    fn clone_box(&self) -> Box<dyn WaitStrategy> {
        Box::new(self.clone())
    }
}


/// A `WaitStrategy` that uses a `Condvar` to block consumer threads when no events are available.
#[derive(Clone, Default)] // Default is now derived for simplicity
pub struct BlockingWaitStrategy {
    notification_pair: Arc<(Mutex<bool>, Condvar)>, 
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
        gating_sequences: &[Arc<Sequence>], // <-- 修改签名
        _consumer_sequence: Arc<Sequence>,
    ) -> i64 {
        let (lock, cvar) = &*self.notification_pair; 
        let mut ready_flag = lock.lock().unwrap(); 

        loop {
            let available_sequence = get_minimum_sequence(gating_sequences);

            if available_sequence >= sequence {
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

    fn clone_box(&self) -> Box<dyn WaitStrategy> {
        Box::new(self.clone())
    }
}


/// A `WaitStrategy` that repeatedly calls `thread::yield_now()` when waiting.
#[derive(Clone, Default)]
pub struct YieldingWaitStrategy;

impl WaitStrategy for YieldingWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        gating_sequences: &[Arc<Sequence>], // <-- 修改签名
        _consumer_sequence: Arc<Sequence>,
    ) -> i64 {
        loop {
            let available_sequence = get_minimum_sequence(gating_sequences);

            if available_sequence >= sequence {
                return available_sequence;
            }

            thread::yield_now();
        }
    }

    fn signal_all(&self) {
        // 不需要显式信号
    }

    fn clone_box(&self) -> Box<dyn WaitStrategy> {
        Box::new(self.clone())
    }
}


/// A `WaitStrategy` that combines busy-spinning, yielding, and blocking phases.
#[derive(Clone)]
pub struct PhasedBackoffWaitStrategy {
    spin_timeout: Duration,
    yield_timeout: Duration,
    
    busy_spin_strategy: BusySpinWaitStrategy,
    yielding_strategy: YieldingWaitStrategy, 
    blocking_strategy: BlockingWaitStrategy, 
}

impl Default for PhasedBackoffWaitStrategy {
    fn default() -> Self {
        Self::new(
            Duration::from_micros(500), 
            Duration::from_millis(5),  
            BusySpinWaitStrategy::default(),
            YieldingWaitStrategy::default(), 
            BlockingWaitStrategy::default(),
        )
    }
}

impl PhasedBackoffWaitStrategy {
    pub fn new(
        spin_timeout: Duration,
        yield_timeout: Duration,
        busy_spin_strategy: BusySpinWaitStrategy,
        yielding_strategy: YieldingWaitStrategy, 
        blocking_strategy: BlockingWaitStrategy,
    ) -> Self {
        PhasedBackoffWaitStrategy {
            spin_timeout,
            yield_timeout,
            busy_spin_strategy,
            yielding_strategy, 
            blocking_strategy,
        }
    }
}

impl WaitStrategy for PhasedBackoffWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        gating_sequences: &[Arc<Sequence>], // <-- 修改签名
        consumer_sequence: Arc<Sequence>, 
    ) -> i64 {
        // Phase 1: Busy Spin
        let spin_start_time = Instant::now();
        loop {
            let available_sequence = get_minimum_sequence(gating_sequences);

            if available_sequence >= sequence {
                return available_sequence;
            }

            if spin_start_time.elapsed() > self.spin_timeout {
                break; 
            }
            hint::spin_loop();
        }

        // Phase 2: Yielding 
        let yield_start_time = Instant::now();
        loop {
            let available_sequence = get_minimum_sequence(gating_sequences);

            if available_sequence >= sequence {
                return available_sequence;
            }

            if yield_start_time.elapsed() > self.yield_timeout {
                break; 
            }
            thread::yield_now(); 
        }

        // Phase 3: Blocking
        self.blocking_strategy.wait_for(sequence, gating_sequences, consumer_sequence)
    }

    fn signal_all(&self) {
        self.blocking_strategy.signal_all();
    }

    fn clone_box(&self) -> Box<dyn WaitStrategy> {
        Box::new(self.clone())
    }
}


// --- 辅助函数：获取序列切片中的最小值 ---
// 许多等待策略需要找到所有门控序列中的最小值
fn get_minimum_sequence(sequences: &[Arc<Sequence>]) -> i64 {
    let mut min_sequence = i64::MAX;
    for s in sequences.iter() {
        min_sequence = min_sequence.min(s.get());
    }
    min_sequence
}
