// src/disruptor.rs

use crate::event::Event;
use crate::ring_buffer::RingBuffer;
// 引入 Sequencer 和 Sequence 类型
use crate::sequencer::{Sequence, Sequencer}; 
// 引入 WaitStrategy trait
use crate::wait_strategy::WaitStrategy; 
use std::sync::Arc;

/// The main `Disruptor` struct, orchestrating all components.
///
/// It sets up the `RingBuffer`, `Sequencer`, and allows for
/// creating `Producer` and `Consumer` instances.
pub struct Disruptor<T: Event, W: WaitStrategy> {
    ring_buffer: Arc<RingBuffer<T>>,
    sequencer: Arc<Sequencer>,
    // 存储 WaitStrategy 的 Arc<dyn WaitStrategy> trait 对象，以便在内部传递和共享
    wait_strategy_arc: Arc<W>, // 保持为具体类型 W，但在需要 Arc<dyn WaitStrategy> 时再转换
}

impl<T: Event, W: WaitStrategy> Disruptor<T, W> {
    /// Creates a new `Disruptor` instance.
    ///
    /// # Arguments
    /// * `capacity`: The size of the ring buffer. Must be a power of two.
    /// * `wait_strategy`: The wait strategy to be used by consumers.
    pub fn new(capacity: usize, wait_strategy: W) -> Self {
        let ring_buffer = Arc::new(RingBuffer::new(capacity));
        
        // 将具体的 wait_strategy 实例封装到 Arc<dyn WaitStrategy> trait 对象中
        // 注意：Sequencer 需要 Arc<dyn WaitStrategy>
        let wait_strategy_arc = Arc::new(wait_strategy); 

        // Sequencer 的构造函数现在需要 wait_strategy_arc
        let sequencer = Arc::new(Sequencer::new(capacity, Arc::clone(&wait_strategy_arc) as Arc<dyn WaitStrategy>));

        Disruptor {
            ring_buffer,
            sequencer,
            wait_strategy_arc, // 存储 Arc<W>
        }
    }

    /// Creates a new `Producer` for this Disruptor.
    ///
    /// # Returns
    /// A `Producer` struct instance.
    pub fn create_producer(&self) -> crate::producer::Producer<T> {
        crate::producer::Producer::new(
            Arc::clone(&self.sequencer),
            Arc::clone(&self.ring_buffer),
        )
    }

    /// Creates a new `Consumer` for this Disruptor.
    ///
    /// 当消费者被创建时，它的序列号会自动注册到 `Sequencer` 作为门控序列。
    ///
    /// # Arguments
    /// * `dependent_sequence`: Optional `Arc<Sequence>` of another consumer
    ///   this one depends on.
    ///
    /// # Returns
    /// A `Consumer` struct instance.
    pub fn create_consumer(&mut self, dependent_sequence: Option<Arc<Sequence>>) -> crate::consumer::Consumer<T, W>
    where
        W: Clone, // WaitStrategy 需要是 Clone 来传递给每个消费者
    {
        // 消费者构造函数需要：producer_cursor, ring_buffer, wait_strategy (克隆), dependent_sequence
        let consumer = crate::consumer::Consumer::new(
            // --- 修正点：使用 producer_cursor ---
            Arc::clone(&self.sequencer.producer_cursor), // 消费者需要 producer_cursor
            Arc::clone(&self.ring_buffer),
            self.wait_strategy_arc.as_ref().clone(), // 克隆 Arc 内部的具体策略 W
            dependent_sequence,
        );
        // 注册此消费者的序列号到主 Sequencer
        self.sequencer.add_gating_sequence(Arc::clone(&consumer.sequence));
        consumer
    }
}
