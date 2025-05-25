// src/disruptor.rs

use crate::event::Event;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::{Sequence, Sequencer};
use crate::wait_strategy::WaitStrategy;
use std::sync::Arc;

/// The main `Disruptor` struct, orchestrating all components.
///
/// It sets up the `RingBuffer`, `Sequencer`, and allows for
/// creating `Producer` and `Consumer` instances.
pub struct Disruptor<T: Event, W: WaitStrategy> {
    ring_buffer: Arc<RingBuffer<T>>,
    sequencer: Arc<Sequencer>,
    wait_strategy: W,
}

impl<T: Event, W: WaitStrategy> Disruptor<T, W> {
    /// Creates a new `Disruptor` instance.
    ///
    /// # Arguments
    /// * `capacity`: The size of the ring buffer. Must be a power of two.
    /// * `wait_strategy`: The wait strategy to be used by consumers.
    pub fn new(capacity: usize, wait_strategy: W) -> Self {
        let ring_buffer = Arc::new(RingBuffer::new(capacity));
        let sequencer = Arc::new(Sequencer::new(capacity));

        Disruptor {
            ring_buffer,
            sequencer,
            wait_strategy,
        }
    }

    /// Creates a new `Producer` for this Disruptor.
    ///
    /// Multiple producers would typically be managed by a `ProducerBarrier`
    /// in a full Disruptor implementation, but for our single-producer model,
    /// this is sufficient.
    pub fn create_producer(&self) -> crate::producer::Producer<T> {
        crate::producer::Producer::new(
            Arc::clone(&self.sequencer),
            Arc::clone(&self.ring_buffer),
        )
    }

    /// Creates a new `Consumer` for this Disruptor.
    ///
    /// When a consumer is created, its sequence is automatically
    /// registered with the `Sequencer` as a gating sequence.
    ///
    /// # Arguments
    /// * `dependent_sequence`: Optional `Arc<Sequence>` of another consumer
    ///   this one depends on.
    pub fn create_consumer(&mut self, dependent_sequence: Option<Arc<Sequence>>) -> crate::consumer::Consumer<T, W>
    where
        W: Clone, // WaitStrategy needs to be Clone to be passed to each consumer
    {
        let consumer = crate::consumer::Consumer::new(
            Arc::clone(&self.sequencer.cursor), // Consumer needs producer's cursor
            Arc::clone(&self.ring_buffer),
            self.wait_strategy.clone(), // Clone the wait strategy for each consumer
            dependent_sequence,
        );
        // Register this consumer's sequence with the main sequencer
        self.sequencer.add_gating_sequence(Arc::clone(&consumer.sequence));
        consumer
    }
}

