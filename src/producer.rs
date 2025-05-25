// src/producer.rs (Modified - minor)

use crate::event::Event;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::Sequencer;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct Producer<T: Event> {
    sequencer: Arc<Sequencer>,
    ring_buffer: Arc<RingBuffer<T>>,
    _marker: PhantomData<T>,
}

impl<T: Event> Producer<T> {
    pub fn new(sequencer: Arc<Sequencer>, ring_buffer: Arc<RingBuffer<T>>) -> Self {
        Producer {
            sequencer,
            ring_buffer,
            _marker: PhantomData,
        }
    }

    /// Claims the next available sequence number from the `Sequencer`.
    /// Now uses `next_batch` internally for multi-producer safety.
    pub fn next(&self) -> i64 {
        self.sequencer.next_batch(1) // Changed from .next() to .next_batch(1)
    }

    pub fn get_mut(&self, sequence: i64) -> &mut T {
        self.ring_buffer.get_mut(sequence)
    }

    pub fn publish(&self, sequence: i64) {
        self.sequencer.publish(sequence);
    }
}
