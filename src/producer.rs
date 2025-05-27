// src/producer.rs

use std::sync::Arc;
use std::marker::PhantomData;

use crate::event::Event; 
use crate::ring_buffer::RingBuffer; 
use crate::sequencer::{Sequencer, ClaimedSequenceGuard}; 

pub struct Producer<T: Event> {
    pub sequencer: Arc<Sequencer>,
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

    pub fn next(&self) -> ClaimedSequenceGuard {
        self.sequencer.next() 
    }
    
    /// Gets a mutable reference to an event in the `RingBuffer` at the given sequence.
    ///
    /// # Safety
    /// This function is `unsafe` because it provides a mutable reference to an event
    /// slot that might be concurrently accessed or is not yet valid for writing by this producer.
    /// The caller **must** ensure the following conditions are met:
    ///
    /// 1. The `sequence` provided was obtained from the `Sequencer` (e.g., via `ClaimedSequenceGuard` from `producer.next()`).
    /// 2. No other producer or consumer is currently writing to or reading from this specific `sequence` slot
    ///    until the `ClaimedSequenceGuard` (which "owns" the right to publish this sequence) is published.
    /// 3. The `sequence` must be one that this producer has claimed and not yet published.
    ///
    /// The `Sequencer` and `ClaimedSequenceGuard` mechanisms are designed to uphold these guarantees
    /// when used correctly. Misusing this function by providing an arbitrary sequence number can lead
    /// to data races or undefined behavior.
    #[allow(clippy::mut_from_ref)] // Allow lint here as well, as it delegates
    pub unsafe fn get_mut(&self, sequence: i64) -> &mut T { unsafe {
        self.ring_buffer.get_mut(sequence) // Calls the unsafe RingBuffer::get_mut
    }}
}

