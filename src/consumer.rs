// src/consumer.rs

use std::sync::Arc;
use std::ops::Fn; 
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
    pub gating_sequences_for_wait: Vec<Arc<Sequence>>, // 包含所有门控条件 (生产者光标 + 依赖)
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
    pub fn process_event<F>(&self, event_handler: F) -> Option<i64>
    where
        F: Fn(&T), 
    {
        let next_sequence_to_consume = self.sequence.get() + 1; 

        // --- 修正点：将 Arc::clone(&self.sequencer) 作为第二个参数传递 ---
        let available_sequence = self.wait_strategy.wait_for(
            next_sequence_to_consume,
            Arc::clone(&self.sequencer), // <-- 新增此参数
            &self.gating_sequences_for_wait, 
            Arc::clone(&self.sequence), 
        );

        if next_sequence_to_consume <= available_sequence {
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
}
