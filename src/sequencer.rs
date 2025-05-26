// src/sequencer.rs

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::hint;

use crate::wait_strategy::WaitStrategy;

// --- Sequence Struct ---
#[derive(Debug)]
#[repr(align(64))] // <-- Add cache line alignment
pub struct Sequence(AtomicI64);

impl Sequence {
    pub fn new(initial_value: i64) -> Self {
        Sequence(AtomicI64::new(initial_value))
    }

    #[inline(always)]
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Release);
    }

    pub fn fetch_add(&self, delta: i64) -> i64 {
        self.0.fetch_add(delta, Ordering::SeqCst) // fetch_add returns previous value
    }
}

#[derive(Debug)]
#[repr(align(64))]
struct PaddedAtomicI64 {
    atomic: AtomicI64,
}

impl PaddedAtomicI64 {
    fn new(initial_value: i64) -> Self {
        PaddedAtomicI64 {
            atomic: AtomicI64::new(initial_value),
        }
    }

    fn load(&self, order: Ordering) -> i64 {
        self.atomic.load(order)
    }

    fn store(&self, value: i64, order: Ordering) {
        self.atomic.store(value, order)
    }
}

// In src/sequencer.rs or a new src/producer_mode.rs

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProducerMode {
    Single,
    Multi,
}

// --- Sequencer Struct ---
pub struct Sequencer {
    pub producer_cursor: Arc<Sequence>,
    pub gating_sequences: RwLock<Vec<Arc<Sequence>>>,
    pub buffer_size: i64,
    index_mask: i64,
    index_shift: i64,
    available_buffer: Vec<PaddedAtomicI64>, // <-- MODIFIED: Use PaddedAtomicI64
    wait_strategy: Arc<dyn WaitStrategy>,
    producer_mode: ProducerMode, // <-- New field
    // --- New field for SPSC fast path ---
    single_consumer_gate_cache: RwLock<Option<Arc<Sequence>>>,
}

impl Sequencer {
    pub fn new(buffer_size: usize, wait_strategy: Arc<dyn WaitStrategy>, producer_mode: ProducerMode) -> Self {
        assert!(buffer_size > 0, "Buffer size must be greater than 0");
        assert!(buffer_size.is_power_of_two(), "Buffer size must be a power of two");

        let capacity_i64 = buffer_size as i64;
        let index_shift = (buffer_size as f64).log2() as i64;

        // Initialize available_buffer with PaddedAtomicI64
        let mut available_buffer_init = Vec::with_capacity(buffer_size);
        for _ in 0..buffer_size {
            available_buffer_init.push(PaddedAtomicI64::new(-1)); // <-- MODIFIED
        }

        Sequencer {
            producer_cursor: Arc::new(Sequence::new(-1)),
            gating_sequences: RwLock::new(Vec::new()),
            buffer_size: capacity_i64,
            index_mask: (capacity_i64 - 1),
            index_shift,
            available_buffer: available_buffer_init, // <-- MODIFIED
            wait_strategy,
            producer_mode, // <-- Store it
            single_consumer_gate_cache: RwLock::new(None), // Initialize new cache
        }
    }

    pub fn add_gating_sequence(&self, sequence: Arc<Sequence>) {
        let mut gating_sequences_guard = self.gating_sequences.write().unwrap();
        gating_sequences_guard.push(Arc::clone(&sequence));

        // Update the single consumer cache
        let mut cache_writer = self.single_consumer_gate_cache.write().unwrap();
        if self.producer_mode == ProducerMode::Single && gating_sequences_guard.len() == 1 {
            *cache_writer = Some(sequence); // Cache the single sequence
        } else {
            *cache_writer = None; // Invalidate cache if not SPSC or multiple consumers
        }
    }

    pub fn next(self: &Arc<Self>) -> ClaimedSequenceGuard {
        self.next_batch(1)
    }

    pub fn next_batch(self: &Arc<Self>, delta: i64) -> ClaimedSequenceGuard {
        if delta <= 0 || delta > self.buffer_size {
            panic!("Delta must be greater than 0 and less than or equal to buffer size.");
        }

        let claimed_sequence_end = match self.producer_mode {
            ProducerMode::Single => self.claim_sequence_single_producer(delta),
            ProducerMode::Multi => self.claim_sequence_multi_producer(delta),
        };

        ClaimedSequenceGuard::new(claimed_sequence_end, Arc::clone(self))
    }

    /// Claims a sequence range for a single producer.
    /// No CAS loop needed for producer_cursor update, but still waits for consumers.
    fn claim_sequence_single_producer(self: &Arc<Self>, delta: i64) -> i64 {
        let current_producer_val = self.producer_cursor.get();
        let next_high_sequence = current_producer_val + delta;
        let wrap_point = next_high_sequence - self.buffer_size;

        // Wait for consumers to advance if the buffer is about to wrap
        // This ensures the slot to be claimed is available (not yet overwritten)
        while wrap_point > self.get_minimum_gating_sequence() {
            // This spin waits for the slowest consumer to pass the point that would be overwritten.
            // In a high-contention or long-wait scenario, a more sophisticated wait (e.g., yield or park)
            // could be considered, but spin_loop is common for short waits in producer paths.
            hint::spin_loop();
        }

        // Single producer can directly set the cursor after ensuring space.
        self.producer_cursor.set(next_high_sequence);
        next_high_sequence
    }

    /// Claims a sequence range for multiple producers using CAS.
    fn claim_sequence_multi_producer(self: &Arc<Self>, delta: i64) -> i64 {
            let mut current_claimed;
            let mut claimed_sequence_end;
            let mut attempts = 0;
    
            loop {
                current_claimed = self.producer_cursor.get();
                claimed_sequence_end = current_claimed + delta;
    
                let wrap_point = claimed_sequence_end - self.buffer_size;
                let min_gating_sequence = self.get_minimum_gating_sequence();
    
                if wrap_point > min_gating_sequence {
                    attempts += 1;
                    if attempts > 1_000_000 { // Heuristic to prevent overly tight spin
                        std::thread::yield_now();
                        attempts = 0;
                    }
                    hint::spin_loop();
                    continue;
                }
    
                // Attempt to claim the sequence range using Compare-And-Swap
                match self.producer_cursor.0.compare_exchange(
                    current_claimed,
                    claimed_sequence_end,
                    Ordering::SeqCst, // Strongest ordering for claiming
                    Ordering::Acquire, // Weaker ordering on failure is fine
                ) {
                    Ok(_) => break, // Successfully claimed
                    Err(_) => {
                        hint::spin_loop(); // Spin and retry on contention
                    }
                }
            }
            claimed_sequence_end
    }

    fn mark_sequence_as_published(self: &Arc<Self>, sequence: i64) {
        let index = (sequence & self.index_mask) as usize;
        let flag_value = sequence >> self.index_shift;
        self.available_buffer[index].store(flag_value, Ordering::Release); // <-- MODIFIED: Uses PaddedAtomicI64's store

        self.wait_strategy.signal_all();
    }

    pub fn get_minimum_gating_sequence(&self) -> i64 {
        // --- SPSC Fast Path ---
        if self.producer_mode == ProducerMode::Single {
            // Try to get a read lock on the cache.
            if let Ok(cache_reader) = self.single_consumer_gate_cache.read() {
                // cache_reader is an RwLockReadGuard<Option<Arc<Sequence>>>
                // We need to get an Option<&Arc<Sequence>> from it to access the Arc<Sequence>
                if let Some(cached_arc_sequence) = cache_reader.as_ref() {
                    // cached_arc_sequence is now &Arc<Sequence>
                    return cached_arc_sequence.get();
                }
            }
            // If cache_reader fails (poisoned RwLock) or cache is None,
            // fall through to the general path.
        }

        let gating_sequences_guard = self.gating_sequences.read().unwrap();
        if gating_sequences_guard.is_empty() {
            // If no consumers, the "barrier" is effectively the producer's own cursor,
            // but producers wait for consumers not to wrap the buffer *ahead* of them.
            // So, if there are no consumers, they can't be behind.
            // A producer should not be able to claim beyond its current_claimed + buffer_size.
            // The check `wrap_point > min_gating_sequence` handles this implicitly.
            // Returning -1 (initial sequence value) means any wrap_point > -1 will be true if buffer is full.
            // However, Java Disruptor often returns producer_cursor itself if no gating sequences,
            // making wrap_point > producer_cursor. This needs careful thought.
            // For now, let's make it behave as if a consumer is at -1.
            return -1; // No barrier from consumers, effectively allowing full buffer use up to producer.
                       // Or return self.producer_cursor.get() but this can lead to deadlock if not careful with wrap_point logic.
                       // The current logic means `wrap_point > -1` which is usually true unless claimed_sequence_end is < buffer_size.
        }

        let mut min_sequence = i64::MAX;
        for gating_sequence in gating_sequences_guard.iter() {
            min_sequence = min_sequence.min(gating_sequence.get());
        }
        min_sequence
    }

    pub fn get_highest_available_sequence(&self, consumer_sequence: i64) -> i64 {
        let highest_claimed_by_producer = self.producer_cursor.get();
        let mut next_sequence_to_check = consumer_sequence + 1;

        while next_sequence_to_check <= highest_claimed_by_producer {
            let index = (next_sequence_to_check & self.index_mask) as usize;
            let expected_flag_value = next_sequence_to_check >> self.index_shift;

            if self.available_buffer[index].load(Ordering::Acquire) == expected_flag_value { // <-- MODIFIED
                // This slot is available
                next_sequence_to_check += 1;
            } else {
                // This slot is not yet available (or a later one), so the highest contiguous is one less
                return next_sequence_to_check - 1;
            }
        }
        // All slots up to and including highest_claimed_by_producer are available
        highest_claimed_by_producer
    }
}

// --- ClaimedSequenceGuard Struct ---
pub struct ClaimedSequenceGuard {
    sequence: i64,
    // Flag to ensure publish is called. Using AtomicI64 for consistency, could be AtomicBool.
    published_flag: AtomicI64,
    sequencer: Arc<Sequencer>,
}

impl ClaimedSequenceGuard {
    fn new(sequence: i64, sequencer: Arc<Sequencer>) -> Self {
        ClaimedSequenceGuard {
            sequence,
            published_flag: AtomicI64::new(0), // 0 for not published, 1 for published
            sequencer,
        }
    }

    pub fn sequence(&self) -> i64 {
        self.sequence
    }

    pub fn publish(self) { // Consumes self to ensure it's called once
        // Mark as published before calling sequencer to avoid race in drop if sequencer panics
        self.published_flag.store(1, Ordering::Relaxed); // Relaxed is fine, it's just for drop check
        self.sequencer.mark_sequence_as_published(self.sequence);
        // Prevent Drop from running by forgetting self.
        std::mem::forget(self);
    }
}

impl Drop for ClaimedSequenceGuard {
    fn drop(&mut self) {
        // This check is now slightly less robust due to std::mem::forget in publish.
        // If publish itself panics *after* published_flag.store but *before* mem::forget,
        // this would still trigger. However, if mark_sequence_as_published panics,
        // the sequence would not be published.
        // The original check `if self.published_flag.load(Ordering::Acquire) == 0` is okay.
        if self.published_flag.load(Ordering::Relaxed) == 0 {
            // This sequence was claimed but not published. This is a critical error
            // as consumers might stall indefinitely waiting for this sequence.
            // Consider if a different action is needed, e.g., trying to publish a
            // "poison pill" or a default event, though that complicates event logic.
            // For now, an error message is the standard approach.
            eprintln!(
                "CRITICAL ERROR: ClaimedSequenceGuard for sequence {} was dropped without being published. Consumers may stall!",
                self.sequence
            );
            // In a real application, you might want to panic or take more drastic action.
        }
    }
}