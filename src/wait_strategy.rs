// src/wait_strategy.rs

use crate::sequencer::Sequence;
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::hint;
use std::time::{Instant, Duration};

/// Trait defining the interface for different wait strategies.
///
/// 实现了 Send + Sync + 'static trait。
/// 'Clone' 和 'Default' 已从 supertrait 中移除，以使其 'dyn' 兼容。
pub trait WaitStrategy: Send + Sync + 'static { // <-- 移除 Clone 和 Default
    /// Waits until the `consumer_sequence` is less than or equal to the `gating_sequence`.
    // (wait_for 方法保持不变)
    fn wait_for(
        &self,
        sequence: i64,
        cursor: Arc<Sequence>,
        dependent_sequence: Option<Arc<Sequence>>,
        consumer_sequence: Arc<Sequence>,
    ) -> i64;

    /// Signals all waiting consumers that new events might be available.
    // (signal_all 方法保持不变)
    fn signal_all(&self);

    /// Clones the trait object into a `Box<dyn WaitStrategy>`.
    /// This method is required because `dyn WaitStrategy` itself cannot derive Clone.
    fn clone_box(&self) -> Box<dyn WaitStrategy>; // <-- 新增方法
}

// --- 为 `Box<dyn WaitStrategy>` 实现 Clone trait ---
// 这使得 `Arc<dyn WaitStrategy>` 可以被克隆。
impl Clone for Box<dyn WaitStrategy> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}


/// A `WaitStrategy` that continuously busy-spins until the event is available.
#[derive(Clone, Default)] // Clone 和 Default 仍为具体实现所需
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

    fn clone_box(&self) -> Box<dyn WaitStrategy> { // <-- 实现 clone_box
        Box::new(self.clone())
    }
}


/// A `WaitStrategy` that uses a `Condvar` to block consumer threads when no events are available.
#[derive(Clone, Default)] // Clone 和 Default 仍为具体实现所需
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

    fn clone_box(&self) -> Box<dyn WaitStrategy> { // <-- 实现 clone_box
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
            thread::yield_now(); 
        }
    }

    fn signal_all(&self) {
        // 不需要显式信号
    }

    fn clone_box(&self) -> Box<dyn WaitStrategy> { // <-- 实现 clone_box
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

        // Phase 2: Yielding 
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
            thread::yield_now(); 
        }

        // Phase 3: Blocking
        self.blocking_strategy.wait_for(sequence, cursor, dependent_sequence, consumer_sequence)
    }

    fn signal_all(&self) {
        self.blocking_strategy.signal_all();
    }

    fn clone_box(&self) -> Box<dyn WaitStrategy> { // <-- 实现 clone_box
        Box::new(self.clone())
    }
}
