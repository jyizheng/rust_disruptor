// src/consumer.rs

use crate::event::Event;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::Sequence;
use crate::wait_strategy::WaitStrategy;
use std::sync::Arc;

/// The `Consumer` reads events from the `RingBuffer` and processes them.
///
/// Each consumer maintains its own `Sequence` to track its progress
/// and uses a `WaitStrategy` to efficiently wait for new events.
pub struct Consumer<T: Event, W: WaitStrategy> {
    /// The consumer's own sequence, tracking the last event it processed.
    pub sequence: Arc<Sequence>,
    /// Shared reference to the producer's cursor.
    cursor: Arc<Sequence>,
    /// Shared reference to the `RingBuffer` from which to read events.
    ring_buffer: Arc<RingBuffer<T>>,
    /// The strategy used to wait for new events.
    wait_strategy: W,
    /// A dependent sequence. If this consumer relies on another consumer
    /// processing an event first, that consumer's sequence goes here.
    /// For now, we'll keep it simple and assume no direct dependencies unless specified.
    dependent_sequence: Option<Arc<Sequence>>,
}

impl<T: Event, W: WaitStrategy> Consumer<T, W> {
    /// Creates a new `Consumer`.
    ///
    /// # Arguments
    /// * `cursor`: The producer's cursor, to know what's available.
    /// * `ring_buffer`: The shared `RingBuffer`.
    /// * `wait_strategy`: The chosen wait strategy for this consumer.
    /// * `dependent_sequence`: Optional sequence of another consumer this one depends on.
    pub fn new(
        cursor: Arc<Sequence>,
        ring_buffer: Arc<RingBuffer<T>>,
        wait_strategy: W,
        dependent_sequence: Option<Arc<Sequence>>,
    ) -> Self {
        Consumer {
            sequence: Arc::new(Sequence::new(-1)), // Start at -1, just like the producer's cursor
            cursor,
            ring_buffer,
            wait_strategy,
            dependent_sequence,
        }
    }

    /// Attempts to read and process the next available event.
    ///
    /// This method will wait until the event at `next_sequence` is available
    /// (published by the producer and potentially processed by any dependencies).
    ///
    /// # Returns
    /// An `Option<&T>` representing the event that was processed.
    /// In a real Disruptor, this might be a batch processing loop.
    pub fn try_next(&self) -> Option<&T> {
        let next_sequence = self.sequence.get() + 1;

        // Wait for the next event to be available according to the wait strategy
        let available_sequence = self.wait_strategy.wait_for(
            next_sequence,
            Arc::clone(&self.cursor),
            self.dependent_sequence.as_ref().map(Arc::clone),
            Arc::clone(&self.sequence),
        );

        if available_sequence >= next_sequence {
            // Event is available, get it from the ring buffer
            let event = self.ring_buffer.get(next_sequence);

            // Update the consumer's own sequence to reflect that this event
            // has been processed.
            self.sequence.set(next_sequence);

            Some(event)
        } else {
            None // No new event available yet
        }
    }

    /// Processes a single event by applying a closure.
    ///
    /// This provides a convenient way to integrate custom processing logic.
    pub fn process_event<F>(&self, mut processor: F) -> Option<()>
    where
        F: FnMut(&T),
    {
        let next_sequence = self.sequence.get() + 1;

        let available_sequence = self.wait_strategy.wait_for(
            next_sequence,
            Arc::clone(&self.cursor),
            self.dependent_sequence.as_ref().map(Arc::clone),
            Arc::clone(&self.sequence),
        );

        if available_sequence >= next_sequence {
            let event = self.ring_buffer.get(next_sequence);
            processor(event); // Apply the processing function
            self.sequence.set(next_sequence); // Update sequence after processing
            Some(())
        } else {
            None
        }
    }
}

