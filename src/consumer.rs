// src/consumer.rs

use std::sync::Arc;
use crate::event::Event;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::{Sequencer, Sequence};
use crate::wait_strategy::WaitStrategy; 

/// Represents a consumer of events from the Disruptor.
pub struct Consumer<T: Event, W: WaitStrategy> {
    pub sequence: Arc<Sequence>, 
    pub sequencer: Arc<Sequencer>,
    pub ring_buffer: Arc<RingBuffer<T>>,
    pub wait_strategy: Arc<W>, 
    pub gating_sequences_for_wait: Vec<Arc<Sequence>>, // Contains all gating conditions (producer cursor + dependencies)
}

impl<T: Event, W: WaitStrategy> Consumer<T, W> {
    pub fn new(
        sequencer: Arc<Sequencer>,
        ring_buffer: Arc<RingBuffer<T>>,
        wait_strategy: W, 
        dependent_sequences: Vec<Arc<Sequence>>, 
    ) -> Self {
        let consumer_sequence = Arc::new(Sequence::new(-1)); 

        let mut gating_sequences = Vec::new();
        gating_sequences.push(Arc::clone(&sequencer.producer_cursor)); 
        for dep_seq in dependent_sequences {
            gating_sequences.push(dep_seq);
        }

        Consumer {
            sequence: consumer_sequence,
            sequencer,
            ring_buffer,
            wait_strategy: Arc::new(wait_strategy), 
            gating_sequences_for_wait: gating_sequences,
        }
    }

    /// Processes the next available event from the RingBuffer.
    pub fn process_event<F>(&self, mut event_handler: F) -> Option<i64>
    where
        F: FnMut(&T), 
    {
        let next_sequence_to_consume = self.sequence.get() + 1; 

        let available_sequence = self.wait_strategy.wait_for(
            next_sequence_to_consume,
            Arc::clone(&self.sequencer),
            &self.gating_sequences_for_wait, 
            Arc::clone(&self.sequence), 
        );

        if next_sequence_to_consume <= available_sequence {
            // Add unsafe block here because RingBuffer::get() is an unsafe function
            // and the Consumer upholds the safety contract for this call.
            unsafe {
                let event = self.ring_buffer.get(next_sequence_to_consume);
                event_handler(event);
            }
            
            self.sequence.set(next_sequence_to_consume);
            Some(next_sequence_to_consume)
        } else {
            None 
        }
    }

    /// Processes a batch of available events from the RingBuffer.
    ///
    /// The `event_handler` is called for each event in the batch.
    /// It receives the event, its sequence number, and a flag indicating if it's the last event in the current batch.
    ///
    /// Returns the number of events processed, or 0 if no events were immediately available.
    pub fn process_event_batch<F>(&self, mut event_handler: F) -> u64
    where
        F: FnMut(&T, i64, bool),
    {
        let next_sequence_to_consume = self.sequence.get() + 1;

        // Wait for the highest sequence available for consumption.
        // This call blocks or spins according to the wait strategy until at least `next_sequence_to_consume` is available.
        let highest_available_sequence = self.wait_strategy.wait_for(
            next_sequence_to_consume,
            Arc::clone(&self.sequencer),
            &self.gating_sequences_for_wait,
            Arc::clone(&self.sequence),
        );

        if highest_available_sequence >= next_sequence_to_consume {
            let mut processed_count = 0;
            for current_sequence in next_sequence_to_consume..=highest_available_sequence {
                let end_of_batch = current_sequence == highest_available_sequence;
                unsafe {
                    // Safety: The wait_strategy ensures that sequences up to `highest_available_sequence`
                    // have been published and are safe to read.
                    let event = self.ring_buffer.get(current_sequence); //
                    event_handler(event, current_sequence, end_of_batch);
                }
                processed_count += 1;
            }
            self.sequence.set(highest_available_sequence); // Update consumer's sequence to the last processed event.
            processed_count
        } else {
            0 // No events were processed.
        }
    }

}
