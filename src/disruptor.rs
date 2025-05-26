// src/disruptor.rs

use crate::event::Event;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::{Sequence, Sequencer}; 
use crate::wait_strategy::WaitStrategy; 
use std::sync::Arc;

pub struct Disruptor<T: Event, W: WaitStrategy> {
    ring_buffer: Arc<RingBuffer<T>>,
    sequencer: Arc<Sequencer>,
    wait_strategy_arc: Arc<dyn WaitStrategy>,
    concrete_wait_strategy: W, // <-- 新增字段
}

impl<T: Event, W: WaitStrategy> Disruptor<T, W> where W: Clone {
    pub fn new(capacity: usize, wait_strategy: W) -> Self {
        let ring_buffer = Arc::new(RingBuffer::new(capacity));
        
        let concrete_wait_strategy_clone = wait_strategy.clone(); 

        let wait_strategy_arc = Arc::new(wait_strategy) as Arc<dyn WaitStrategy>; 

        let sequencer = Arc::new(Sequencer::new(capacity, Arc::clone(&wait_strategy_arc)));

        Disruptor {
            ring_buffer,
            sequencer,
            wait_strategy_arc,
            concrete_wait_strategy: concrete_wait_strategy_clone,
        }
    }

    pub fn create_producer(&self) -> crate::producer::Producer<T> {
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
            self.concrete_wait_strategy.clone(), // 克隆 Arc 内部的具体策略 W
            dependent_sequences, // <-- 修正点：传递依赖序列
        );
        // 注册此消费者的序列号到主 Sequencer
        self.sequencer.add_gating_sequence(Arc::clone(&consumer.sequence));
        consumer
    }
}
