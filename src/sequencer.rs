// src/sequencer.rs

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI64, Ordering};
use std::hint;

use crate::wait_strategy::WaitStrategy;

// --- Sequence Struct ---
#[derive(Debug)]
pub struct Sequence(AtomicI64);

impl Sequence {
    pub fn new(initial_value: i64) -> Self {
        Sequence(AtomicI64::new(initial_value))
    }

    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }

    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Release);
    }

    pub fn fetch_add(&self, delta: i64) -> i64 {
        self.0.fetch_add(delta, Ordering::SeqCst) // fetch_add returns previous value
    }
}

// --- Cache Padded AtomicI64 to prevent false sharing ---
// Common cache line sizes are 64 or 128 bytes.
// Using 64 here as a common default.
#[cfg(all(any(target_arch = "x86_64", target_arch = "aarch64"), target_pointer_width = "64"))]
const CACHE_LINE_SIZE: usize = 64;
#[cfg(not(all(any(target_arch = "x86_64", target_arch = "aarch64"), target_pointer_width = "64")))]
const CACHE_LINE_SIZE: usize = 64; // Default, consider platform specifics

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


// --- Sequencer Struct ---
pub struct Sequencer {
    pub producer_cursor: Arc<Sequence>,
    pub gating_sequences: Mutex<Vec<Arc<Sequence>>>,
    pub buffer_size: i64,
    index_mask: i64,
    index_shift: i64,
    available_buffer: Vec<PaddedAtomicI64>, // <-- MODIFIED: Use PaddedAtomicI64
    wait_strategy: Arc<dyn WaitStrategy>,
}

impl Sequencer {
    pub fn new(buffer_size: usize, wait_strategy: Arc<dyn WaitStrategy>) -> Self {
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
            gating_sequences: Mutex::new(Vec::new()),
            buffer_size: capacity_i64,
            index_mask: (capacity_i64 - 1),
            index_shift,
            available_buffer: available_buffer_init, // <-- MODIFIED
            wait_strategy,
        }
    }

    pub fn add_gating_sequence(&self, sequence: Arc<Sequence>) {
        self.gating_sequences.lock().unwrap().push(sequence);
    }

    pub fn next(self: &Arc<Self>) -> ClaimedSequenceGuard {
        self.next_batch(1)
    }

    pub fn next_batch(self: &Arc<Self>, delta: i64) -> ClaimedSequenceGuard {
        if delta <= 0 || delta > self.buffer_size {
            panic!("Delta must be greater than 0 and less than or equal to buffer size.");
        }

        let mut current_claimed;
        let mut claimed_sequence_end;
        let mut attempts = 0; // To prevent potential infinite spin in some edge cases if logic is flawed

        loop {
            current_claimed = self.producer_cursor.get();
            claimed_sequence_end = current_claimed + delta;

            let wrap_point = claimed_sequence_end - self.buffer_size;
            let min_gating_sequence = self.get_minimum_gating_sequence();

            if wrap_point > min_gating_sequence {
                attempts += 1;
                if attempts > 1_000_000 { // Heuristic to prevent overly tight spin, adjust as needed
                    std::thread::yield_now(); // Yield if spinning too long
                    attempts = 0; // Reset attempts after yielding
                }
                hint::spin_loop();
                continue;
            }

            // Attempt to claim the sequence range
            // Using compare_exchange_weak for potentially better performance in contended loops,
            // but requires a loop for spurious failures. Strong is simpler to reason about.
            // Sticking with strong as per original.
            match self.producer_cursor.0.compare_exchange(
                current_claimed,
                claimed_sequence_end,
                Ordering::SeqCst, // Strongest ordering for claiming
                Ordering::Acquire, // Weaker ordering on failure is fine
            ) {
                Ok(_) => break, // Successfully claimed
                Err(_) => {
                    hint::spin_loop(); // Spin and retry
                }
            }
        }

        ClaimedSequenceGuard::new(claimed_sequence_end, Arc::clone(self))
    }

    fn mark_sequence_as_published(self: &Arc<Self>, sequence: i64) {
        let index = (sequence & self.index_mask) as usize;
        let flag_value = sequence >> self.index_shift;
        self.available_buffer[index].store(flag_value, Ordering::Release); // <-- MODIFIED: Uses PaddedAtomicI64's store

        self.wait_strategy.signal_all();
    }

    pub fn get_minimum_gating_sequence(&self) -> i64 {
        let gating_sequences_guard = self.gating_sequences.lock().unwrap();
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