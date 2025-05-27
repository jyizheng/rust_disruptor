// src/disruptor.rs

use crate::event::Event;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::{Sequence, Sequencer, ProducerMode}; 
use crate::wait_strategy::WaitStrategy; 
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Disruptor<T: Event, W: WaitStrategy> {
    ring_buffer: Arc<RingBuffer<T>>,
    sequencer: Arc<Sequencer>,
    concrete_wait_strategy: W,
    producer_mode: ProducerMode,
    producer_created: AtomicBool,
}

impl<T: Event, W: WaitStrategy> Disruptor<T, W> where W: Clone {
    pub fn new(capacity: usize, wait_strategy: W,  producer_mode: ProducerMode) -> Self {
        let ring_buffer = Arc::new(RingBuffer::new(capacity));
        let concrete_wait_strategy_clone = wait_strategy.clone(); 

        // Create the Arc<dyn WaitStrategy> to pass to the Sequencer
        let wait_strategy_for_sequencer = Arc::new(wait_strategy) as Arc<dyn WaitStrategy>;

        let sequencer = Arc::new(Sequencer::new(capacity, Arc::clone(&wait_strategy_for_sequencer), producer_mode));

        Disruptor {
            ring_buffer,
            sequencer,
            concrete_wait_strategy: concrete_wait_strategy_clone,
            producer_mode,
            producer_created: AtomicBool::new(false),
        }
    }

    /// Returns the producer mode of the Disruptor.
    pub fn producer_mode(&self) -> ProducerMode { // Added public getter
        self.producer_mode
    }

    pub fn create_producer(&self) -> crate::producer::Producer<T> {
        // Enforce that create_producer is called only once for SingleProducer mode
        if self.producer_mode == ProducerMode::Single && self.producer_created.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_err() {
            panic!("Cannot create more than one producer in SingleProducer mode.");
        }

        crate::producer::Producer::new(
            Arc::clone(&self.sequencer),
            Arc::clone(&self.ring_buffer),
        )
    }

    /// Creates a new `Consumer` for this Disruptor.
    ///
    /// # Arguments
    /// * `dependent_sequences`: A `Vec<Arc<Sequence>>` containing sequences of other
    ///   consumers this one depends on. Pass an empty Vec if no dependencies.
    pub fn create_consumer(&mut self, dependent_sequences: Vec<Arc<Sequence>>) -> crate::consumer::Consumer<T, W>
    where
        W: Clone, 
    {
        let consumer = crate::consumer::Consumer::new(
            Arc::clone(&self.sequencer), 
            Arc::clone(&self.ring_buffer),
            self.concrete_wait_strategy.clone(), // Clone the concrete strategy W
            dependent_sequences, // Pass dependent sequences
        );
        // Register this consumer's sequence with the main Sequencer
        self.sequencer.add_gating_sequence(Arc::clone(&consumer.sequence));
        consumer
    }
}
