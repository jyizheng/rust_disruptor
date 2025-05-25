// src/wait_strategy.rs

use crate::sequencer::Sequence;
use std::sync::Arc;
use std::hint; // For `spin_loop_hint`

/// Trait defining the interface for different wait strategies.
pub trait WaitStrategy: Send + Sync + 'static {
    /// Waits until the `consumer_sequence` is less than or equal to the `gating_sequence`.
    ///
    /// This method is called by a consumer to wait for an event at a particular sequence
    /// to become available (i.e., published by the producer and potentially
    /// processed by upstream consumers if there are dependencies).
    ///
    /// # Arguments
    /// * `sequence`: The sequence number the consumer is trying to reach.
    /// * `cursor`: The producer's current published sequence.
    /// * `dependent_sequence`: (Optional) The sequence of a dependent consumer (if any),
    ///   meaning we also need to wait for this consumer to pass the `sequence`.
    /// * `consumer_sequence`: The consumer's own current sequence. Used to update its
    ///   progress if it processes events while waiting.
    fn wait_for(
        &self,
        sequence: i64,
        cursor: Arc<Sequence>,
        dependent_sequence: Option<Arc<Sequence>>,
        consumer_sequence: Arc<Sequence>,
    ) -> i64;
}

/// A `WaitStrategy` that continuously busy-spins until the event is available.
///
/// This strategy offers the lowest latency in low-contention scenarios but
/// consumes 100% CPU when waiting. It's generally not recommended for
/// high-contention or low-frequency scenarios in production.
#[derive(Clone)] // Derive Clone for the BusySpinWaitStrategy
pub struct BusySpinWaitStrategy;

impl WaitStrategy for BusySpinWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        cursor: Arc<Sequence>,
        dependent_sequence: Option<Arc<Sequence>>,
        _consumer_sequence: Arc<Sequence>, // Not used in busy-spin
    ) -> i64 {
        loop {
            // Get the current value of the producer's cursor
            let available_sequence = cursor.get();

            // Check if the event we are waiting for is available
            if available_sequence >= sequence {
                // If there's a dependent sequence, ensure it has also progressed
                // past our target sequence. This is for consumer-to-consumer dependencies.
                if let Some(dep_seq) = &dependent_sequence {
                    if dep_seq.get() >= sequence {
                        return sequence; // Event is available and dependencies met
                    }
                } else {
                    return sequence; // Event is available (no dependencies or dependency met)
                }
            }

            // Hint to the CPU that we are in a busy-wait loop.
            // On x86, this typically translates to a `PAUSE` instruction,
            // which can reduce power consumption and improve performance
            // by preventing pipeline stalls.
            hint::spin_loop();
        }
    }
}

