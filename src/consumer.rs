// src/consumer.rs

use std::sync::Arc;
use std::ops::Fn;
use crate::event::Event;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::{Sequencer, Sequence};
use crate::wait_strategy::WaitStrategy;

/// Represents a consumer of events from the Disruptor.
pub struct Consumer<T: Event, W: WaitStrategy> {
    pub sequence: Arc<Sequence>, // 消费者自己的序列号 (公开以便 Sequencer 门控)
    sequencer: Arc<Sequencer>,
    ring_buffer: Arc<RingBuffer<T>>,
    wait_strategy: Arc<W>, 
    
    // 门控序列：消费者需要等待这些序列号（包括生产者和依赖的消费者）
    gating_sequences_for_wait: Vec<Arc<Sequence>>,
}

impl<T: Event, W: WaitStrategy> Consumer<T, W> {
    pub fn new(
        sequencer: Arc<Sequencer>,
        ring_buffer: Arc<RingBuffer<T>>,
        wait_strategy: W, // 接收具体类型 W
        // --- 修正点：接受 Vec<Arc<Sequence>> 作为依赖 ---
        dependent_sequences: Vec<Arc<Sequence>>, 
    ) -> Self {
        let consumer_sequence = Arc::new(Sequence::new(-1)); 

        let mut gating_sequences = Vec::new();
        // 生产者光标是首要门控条件
        gating_sequences.push(Arc::clone(&sequencer.producer_cursor)); 
        // --- 修正点：添加所有依赖的消费者序列 ---
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

        let available_sequence = self.wait_strategy.wait_for(
            next_sequence_to_consume,
            &self.gating_sequences_for_wait, // 传递包含所有门控条件的切片
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
