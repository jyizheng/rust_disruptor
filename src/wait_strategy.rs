// src/wait_strategy.rs

use crate::sequencer::Sequence;
use crate::sequencer::Sequencer;
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::hint;
use std::time::{Instant, Duration};

/// Trait defining the interface for different wait strategies.
pub trait WaitStrategy: Send + Sync + 'static {
    fn wait_for(
        &self,
        sequence: i64,
        sequencer: Arc<Sequencer>, // <-- 核心修正点：确保此参数存在
        gating_sequences: &[Arc<Sequence>],
        consumer_sequence: Arc<Sequence>,
    ) -> i64;

    fn signal_all(&self);

    fn clone_box(&self) -> Box<dyn WaitStrategy>;
}

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
        sequence: i64, // sequence to be consumed
        sequencer: Arc<Sequencer>,
        gating_sequences: &[Arc<Sequence>], // For SPSC, this is just [producer_cursor]
        _consumer_sequence: Arc<Sequence>,
    ) -> i64 {
        loop {
            // Check the producer's progress first (this is cheap)
            let producer_cursor_val = get_minimum_sequence(gating_sequences); // In SPSC, this is producer_cursor.get()

            if producer_cursor_val >= sequence {
                // Only if the producer claims to be ahead, check the available_buffer
                // This is the more expensive check.
                let highest_published_and_gated = sequencer.get_highest_available_sequence(sequence - 1);
                if highest_published_and_gated >= sequence {
                    return highest_published_and_gated;
                }
            }
            hint::spin_loop(); // Spin regardless, but we avoided one expensive call if producer wasn't ahead
        }
    }

    fn signal_all(&self) { }

    fn clone_box(&self) -> Box<dyn WaitStrategy> {
        Box::new(self.clone())
    }
}


/// A `WaitStrategy` that uses a `Condvar` to block consumer threads when no events are available.
#[derive(Clone, Default)]
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
        sequencer: Arc<Sequencer>,
        gating_sequences: &[Arc<Sequence>], 
        _consumer_sequence: Arc<Sequence>,
    ) -> i64 {
        let (lock, cvar) = &*self.notification_pair; 
        let mut ready_flag = lock.lock().unwrap(); 

        loop {
            let min_gating_sequence = get_minimum_sequence(gating_sequences);

            if min_gating_sequence >= sequence {
                let available_from_sequencer = sequencer.get_highest_available_sequence(sequence - 1);
                if available_from_sequencer >= sequence {
                    *ready_flag = false; 
                    return available_from_sequencer; 
                }
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
        sequencer: Arc<Sequencer>,
        gating_sequences: &[Arc<Sequence>], 
        _consumer_sequence: Arc<Sequence>,
    ) -> i64 {
        loop {
            let min_gating_sequence = get_minimum_sequence(gating_sequences);

            if min_gating_sequence >= sequence {
                let available_from_sequencer = sequencer.get_highest_available_sequence(sequence - 1);
                if available_from_sequencer >= sequence {
                    return available_from_sequencer;
                }
            }
            thread::yield_now(); 
        }
    }

    fn signal_all(&self) { }

    fn clone_box(&self) -> Box<dyn WaitStrategy> {
        Box::new(self.clone())
    }
}


/// A `WaitStrategy` that combines busy-spinning, yielding, and blocking phases.
#[derive(Clone, Default)]
pub struct PhasedBackoffWaitStrategy {
    spin_timeout: Duration,
    yield_timeout: Duration,
    blocking_strategy: BlockingWaitStrategy, 
}

impl PhasedBackoffWaitStrategy {
    pub fn new(
        spin_timeout: Duration,
        yield_timeout: Duration,
        blocking_strategy: BlockingWaitStrategy,
    ) -> Self {
        PhasedBackoffWaitStrategy {
            spin_timeout,
            yield_timeout,
            blocking_strategy,
        }
    }
}

impl WaitStrategy for PhasedBackoffWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        sequencer: Arc<Sequencer>,
        gating_sequences: &[Arc<Sequence>], 
        consumer_sequence: Arc<Sequence>, 
    ) -> i64 {
        // Phase 1: Busy Spin
        let spin_start_time = Instant::now();
        loop {
            let min_gating_sequence = get_minimum_sequence(gating_sequences);
            if min_gating_sequence >= sequence {
                let available_from_sequencer = sequencer.get_highest_available_sequence(sequence - 1);
                if available_from_sequencer >= sequence {
                    return available_from_sequencer;
                }
            }
            if spin_start_time.elapsed() > self.spin_timeout {
                break; 
            }
            hint::spin_loop();
        }

        // Phase 2: Yielding 
        let yield_start_time = Instant::now();
        loop {
            let min_gating_sequence = get_minimum_sequence(gating_sequences);
            if min_gating_sequence >= sequence {
                let available_from_sequencer = sequencer.get_highest_available_sequence(sequence - 1);
                if available_from_sequencer >= sequence {
                    return available_from_sequencer;
                }
            }
            if yield_start_time.elapsed() > self.yield_timeout {
                break; 
            }
            thread::yield_now(); 
        }

        // Phase 3: Blocking
        self.blocking_strategy.wait_for(sequence, sequencer, gating_sequences, consumer_sequence)
    }

    fn signal_all(&self) {
        self.blocking_strategy.signal_all();
    }

    fn clone_box(&self) -> Box<dyn WaitStrategy> {
        Box::new(self.clone())
    }
}


// --- Helper function: get the minimum value in a sequence slice ---
fn get_minimum_sequence(sequences: &[Arc<Sequence>]) -> i64 {
    let mut min_sequence = i64::MAX;
    for s in sequences.iter() {
        min_sequence = min_sequence.min(s.get());
    }
    min_sequence
}
