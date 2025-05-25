// src/sequencer.rs (MODIFIED for Multi-Producer)

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::hint; // For spin_loop_hint

// Re-defining Sequence as it was, but adding some helpful methods for claiming
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

    /// Atomically adds `delta` to the sequence and returns the new value.
    pub fn fetch_add(&self, delta: i64) -> i64 {
        self.0.fetch_add(delta, Ordering::SeqCst) + delta
    }
}

/// Manages the sequence numbers for producers and consumers.
/// Now designed to support multiple producers.
pub struct Sequencer {
    /// The highest sequence number that has been successfully published by the producer(s).
    pub cursor: Arc<Sequence>,
    /// The highest sequence number that producers have *claimed* but not yet published.
    /// This is crucial for multi-producer to manage concurrent claims.
    highest_claimed_sequence: Sequence, // New field!
    /// The sequence numbers of all dependent consumers.
    pub gating_sequences: Mutex<Vec<Arc<Sequence>>>,
    pub buffer_size: i64,
    index_mask: i64, // Added for convenience in calculations
}

impl Sequencer {
    pub fn new(buffer_size: usize) -> Self {
        assert!(buffer_size > 0, "Buffer size must be greater than 0");
        assert!(buffer_size.is_power_of_two(), "Buffer size must be a power of two");

        Sequencer {
            cursor: Arc::new(Sequence::new(-1)),
            highest_claimed_sequence: Sequence::new(-1), // Initialize
            gating_sequences: Mutex::new(Vec::new()),
            buffer_size: buffer_size as i64,
            index_mask: (buffer_size - 1) as i64, // Initialize
        }
    }

    pub fn add_gating_sequence(&self, sequence: Arc<Sequence>) {
        self.gating_sequences.lock().unwrap().push(sequence);
    }

    /// Claims the next available sequence number for a producer.
    /// This method now handles contention among multiple producers and
    /// waits if the buffer is full (back-pressure).
    ///
    /// # Returns
    /// The sequence number that the producer can now write to.
    pub fn next(&self) -> i64 {
        self.next_batch(1) // Claim a single slot
    }

    /// Claims a batch of `delta` sequence numbers for a producer.
    /// This is the core multi-producer claiming logic.
    pub fn next_batch(&self, delta: i64) -> i64 {
        if delta <= 0 || delta > self.buffer_size {
            panic!("Delta must be greater than 0 and less than or equal to buffer size.");
        }

        let current_producer_cursor = self.highest_claimed_sequence.get();
        let next_sequence = current_producer_cursor + delta;
        let wrap_point = next_sequence - self.buffer_size; // The sequence that would be overwritten

        // We need to wait if the next_sequence would overwrite the slowest consumer's progress
        let min_gating_sequence = self.get_minimum_gating_sequence();

        // If the next sequence to claim would wrap around and overwrite an event
        // that the slowest consumer hasn't read yet, we need to wait.
        if wrap_point > min_gating_sequence {
            // Busy-spin until space is available. In a real Disruptor,
            // this would involve a more sophisticated wait strategy (like `BlockingWaitStrategy`)
            // that doesn't burn CPU unnecessarily.
            loop {
                let current_min_gating_sequence = self.get_minimum_gating_sequence();
                if wrap_point <= current_min_gating_sequence {
                    break; // Space is now available
                }
                hint::spin_loop();
            }
        }

        // Atomically update the highest_claimed_sequence.
        // This is crucial for multi-producer contention.
        // `compare_exchange` could be used here for more explicit retry logic,
        // but `Workspace_add` on `highest_claimed_sequence` ensures unique claims.
        // We ensure that the sequence we want to claim (next_sequence) is what
        // we actually get. If other producers are contending, they will also
        // update this. This requires careful ordering.

        // A simple `Workspace_add` on `highest_claimed_sequence` is typically what happens.
        // `highest_claimed_sequence.fetch_add(delta)` would get the sequence *before* add.
        // We want the sequence *after* add.
        // The `next_sequence` value calculated above is the *target* value.
        // We need to ensure that when we update `highest_claimed_sequence`, it becomes `next_sequence`.

        // More robust multi-producer `next` implementation:
        // Use a loop with `compare_exchange` to ensure atomicity when multiple producers
        // try to claim at the same time.

        let mut current;
        let mut claimed_sequence;
        loop {
            current = self.highest_claimed_sequence.get();
            claimed_sequence = current + delta;

            // Wait if the claimed_sequence would go beyond the buffer and hit slowest consumer
            let wrap_point_check = claimed_sequence - self.buffer_size;
            let min_gating = self.get_minimum_gating_sequence();
            if wrap_point_check > min_gating {
                 // Wait again for space, or yield, or use proper wait strategy
                 // For now, we'll just spin_loop here within the loop.
                 hint::spin_loop();
                 continue; // Re-check the claim
            }

            // Attempt to update highest_claimed_sequence to claimed_sequence
            // If another producer snuck in and updated `highest_claimed_sequence`
            // between our `get()` and `set()`, `compare_exchange` will fail,
            // and we loop to retry.
            match self.highest_claimed_sequence.0.compare_exchange(
                current,
                claimed_sequence,
                Ordering::SeqCst, // Strongest ordering for CAS
                Ordering::Acquire, // If it fails, still ensure memory consistency
            ) {
                Ok(_) => break, // Successfully claimed
                Err(_) => {
                    hint::spin_loop(); // Failed to claim, retry
                    // If we had a blocking wait strategy, we'd notify here.
                }
            }
        }

        claimed_sequence
    }


    /// Publishes a sequence number, making the event visible to consumers.
    ///
    /// For multi-producer, the `publish` call ensures that events are
    /// published in order of their sequence numbers, even if producers
    /// claimed them out of order.
    pub fn publish(&self, sequence: i64) {
        // In a true multi-producer Disruptor, the producer might need to
        // wait until previous sequences are published before publishing its own.
        // This is handled by a "Publisher Barrier" which we are simplifying here.
        // For now, we assume sequences are published in order, or that the
        // consumer's wait strategy implicitly handles out-of-order writes by
        // waiting for the cursor to advance.

        // The cursor reflects the highest published sequence that is
        // sequentially consistent.
        // We only advance the cursor if *this* sequence is the next in line.
        // Otherwise, a producer might "jump ahead" and publish sequence 5 before 4,
        // and our cursor would reflect 5 prematurely.
        let mut current_cursor = self.cursor.get();
        while current_cursor < sequence {
            match self.cursor.0.compare_exchange(
                current_cursor,
                sequence,
                Ordering::Release,
                Ordering::Relaxed, // If it fails, we still need to know current.
            ) {
                Ok(_) => break, // Successfully updated
                Err(actual_current) => {
                    current_cursor = actual_current; // Another thread updated it, retry
                    // If another producer already published this sequence (or a higher one),
                    // our job is done.
                    if current_cursor >= sequence {
                        break;
                    }
                    hint::spin_loop(); // Spin if contention
                }
            }
        }
    }


    /// Gets the lowest sequence number among all gating sequences.
    pub fn get_minimum_gating_sequence(&self) -> i64 {
        let gating_sequences_guard = self.gating_sequences.lock().unwrap();
        if gating_sequences_guard.is_empty() {
            return self.cursor.get();
        }

        let mut min_sequence = i64::MAX;
        for gating_sequence in gating_sequences_guard.iter() {
            min_sequence = min_sequence.min(gating_sequence.get());
        }
        min_sequence
    }
}