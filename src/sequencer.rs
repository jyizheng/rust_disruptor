// src/sequencer.rs (Modified)

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex}; // Add Mutex import!

/// A sequence number to track progress in the ring buffer.
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

    pub fn increment_and_get(&self) -> i64 {
        self.0.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// Manages the sequence numbers for producers and consumers.
/// It's responsible for claiming slots for producers and tracking how far
/// behind consumers are.
pub struct Sequencer {
    /// The highest sequence number that has been successfully published by the producer.
    pub cursor: Arc<Sequence>,
    /// The sequence numbers of all dependent consumers.
    /// Wrapped in a Mutex for safe interior mutability when accessed via Arc.
    pub gating_sequences: Mutex<Vec<Arc<Sequence>>>, // <--- CHANGED HERE!
    pub buffer_size: i64,
}

impl Sequencer {
    pub fn new(buffer_size: usize) -> Self {
        assert!(buffer_size > 0, "Buffer size must be greater than 0");
        assert!(buffer_size.is_power_of_two(), "Buffer size must be a power of two");

        Sequencer {
            cursor: Arc::new(Sequence::new(-1)),
            gating_sequences: Mutex::new(Vec::new()), // <--- CHANGED HERE!
            buffer_size: buffer_size as i64,
        }
    }

    /// Adds a gating sequence, typically for a consumer.
    /// Now takes `&self` because `gating_sequences` is wrapped in a `Mutex`.
    pub fn add_gating_sequence(&self, sequence: Arc<Sequence>) { // <--- CHANGED HERE!
        self.gating_sequences.lock().unwrap().push(sequence); // <--- CHANGED HERE!
    }

    /// Claims the next available sequence number for publishing.
    pub fn next(&self) -> i64 {
        let next_sequence = self.cursor.increment_and_get();
        next_sequence
    }

    /// Publishes a sequence number, making the event visible to consumers.
    pub fn publish(&self, sequence: i64) {
        self.cursor.set(sequence);
    }

    /// Gets the lowest sequence number among all gating sequences.
    /// This tells us how far behind the slowest consumer is.
    pub fn get_minimum_gating_sequence(&self) -> i64 {
        let gating_sequences_guard = self.gating_sequences.lock().unwrap(); // <--- CHANGED HERE!
        if gating_sequences_guard.is_empty() {
            return self.cursor.get();
        }

        let mut min_sequence = i64::MAX;
        for gating_sequence in gating_sequences_guard.iter() { // <--- CHANGED HERE!
            min_sequence = min_sequence.min(gating_sequence.get());
        }
        min_sequence
    }
}