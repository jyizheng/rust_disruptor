// src/sequencer.rs (MODIFIED for Multi-Producer)

use std::sync::atomic::{AtomicI64, AtomicI8, Ordering};
use std::sync::{Arc, Mutex};
use std::hint; // For spin_loop_hint

// Sequencer needs RingBuffer to define its capacity and index_mask
// It doesn't directly access entries, but these types are relevant to its operation
use crate::ring_buffer::RingBuffer;
// The Event trait is needed for consistency if types related to events are discussed
use crate::event::Event;

// Re-defining Sequence as it was, but adding some helpful methods for claiming
#[derive(Debug)]
pub struct Sequence(AtomicI64);

impl Sequence {
    pub fn new(initial_value: i64) -> Self {
        Sequence(AtomicI64::new(initial_value))
    }

    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }

    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Release);
    }

    /// Atomically adds `delta` to the sequence and returns the new value.
    pub fn fetch_add(&self, delta: i64) -> i64 {
        self.0.fetch_add(delta, Ordering::SeqCst) + delta
    }
}

/// Manages the sequence numbers for producers and consumers.
/// Now designed to support multiple producers.
pub struct Sequencer {
    /// The highest sequence number that has been successfully published by the producer(s).
    pub cursor: Arc<Sequence>,
    /// The highest sequence number that producers have *claimed* but not yet published.
    /// This is crucial for multi-producer to manage concurrent claims.
    highest_claimed_sequence: Sequence, // New field!
    /// The sequence numbers of all dependent consumers.
    pub gating_sequences: Mutex<Vec<Arc<Sequence>>>,
    pub buffer_size: i64,
    index_mask: i64, // Added for convenience in calculations

    /// 跟踪环形缓冲区中每个槽位的发布状态。
    /// 0: 已声明但未发布, 1: 已发布。
    /// 这对于多生产者确保发布连续性至关重要。
    available_buffer: Vec<AtomicI8>,
}

impl Sequencer {
    pub fn new(buffer_size: usize) -> Self {
        assert!(buffer_size > 0, "Buffer size must be greater than 0");
        assert!(buffer_size.is_power_of_two(), "Buffer size must be a power of two");

        let capacity_i64 = buffer_size as i64;
        let available_buffer = (0..buffer_size).map(|_| AtomicI8::new(0)).collect();

        Sequencer {
            cursor: Arc::new(Sequence::new(-1)),
            highest_claimed_sequence: Sequence::new(-1), // Initialize
            gating_sequences: Mutex::new(Vec::new()),
            buffer_size: buffer_size as i64,
            index_mask: (buffer_size - 1) as i64, // Initialize
            available_buffer,
        }
    }

    pub fn add_gating_sequence(&self, sequence: Arc<Sequence>) {
        self.gating_sequences.lock().unwrap().push(sequence);
    }

    /// Claims the next available sequence number for a producer.
    /// This method now handles contention among multiple producers and
    /// waits if the buffer is full (back-pressure).
    ///
    /// # Returns
    /// The sequence number that the producer can now write to.
    pub fn next(self: &Arc<Self>) -> ClaimedSequenceGuard {
        self.next_batch(1) // Claim a single slot
    }


    /// 为生产者声明一批 `delta` 序列号。
    /// 这是核心的多生产者声明逻辑。
    ///
    /// # 返回值
    /// 一个 `ClaimedSequenceGuard`，它封装了声明的序列号，
    /// 并且在被丢弃时（如果未发布）会发出警告。
    pub fn next_batch(self: &Arc<Self>, delta: i64) -> ClaimedSequenceGuard {
        if delta <= 0 || delta > self.buffer_size {
            panic!("Delta must be greater than 0 and less than or equal to buffer size.");
        }

        let mut current_claimed;
        let mut claimed_sequence_end; // 本批次声明的结束序列号

        loop {
            current_claimed = self.highest_claimed_sequence.get(); // 获取当前最高已声明序列号
            claimed_sequence_end = current_claimed + delta; // 计算本批次声明的结束序列号

            // 检查是否会覆盖最慢消费者的进度 (反压逻辑)
            let wrap_point = claimed_sequence_end - self.buffer_size; // 如果发生环绕，将被覆盖的序列号
            let min_gating_sequence = self.get_minimum_gating_sequence();

            // 如果声明的批次将覆盖未被最慢消费者读取的事件，则需要等待。
            if wrap_point > min_gating_sequence {
                // 等待空间可用。在真实的 Disruptor 中，
                // 这将涉及更复杂的等待策略（如 `BlockingWaitStrategy`），
                // 而不是不必要地消耗 CPU。
                // 每次循环迭代都重新检查 `min_gating_sequence`。
                hint::spin_loop(); 
                continue; // 空间不足，继续尝试声明 (再次获取 current_claimed)
            }

            // 尝试原子性地更新 `highest_claimed_sequence`。
            // 这是多生产者竞争的关键。
            match self.highest_claimed_sequence.0.compare_exchange(
                current_claimed,
                claimed_sequence_end,
                Ordering::SeqCst, // 成功的内存顺序
                Ordering::Acquire, // 失败时的内存顺序，确保最新值可见
            ) {
                Ok(_) => break, // 成功声明，退出循环
                Err(_) => {
                    // 另一个生产者在期间更新了 `highest_claimed_sequence`，
                    // 重新尝试声明。
                    hint::spin_loop(); // 竞争激烈，自旋等待
                }
            }
        }
        
        // 返回一个 ClaimedSequenceGuard 实例
        ClaimedSequenceGuard::new(claimed_sequence_end, Arc::clone(self))
    }


    /// 内部方法：标记一个序列号为已发布，使其事件对消费者可见。
    /// 此方法由 `ClaimedSequenceGuard` 调用。
    fn mark_sequence_as_published(self: &Arc<Self>, sequence: i64) {
        // 1. 将当前序列号对应的槽位标记为已发布。
        let index = (sequence & self.index_mask) as usize; // 使用 index_mask 进行快速取模
        self.available_buffer[index].store(1, Ordering::Release); // 设置为1表示已发布

        // 2. 尝试推进最高已发布光标 (cursor)。
        // 光标只有在发现连续的已发布序列号时才会前进。
        let mut current_cursor = self.cursor.get(); // 获取当前最高连续已发布序列号
        loop {
            let next_expected_sequence = current_cursor + 1;
            let next_expected_index = (next_expected_sequence & self.index_mask) as usize;
            
            // 检查下一个预期的序列号是否在已声明范围内
            // (防止尝试推进到尚未被任何生产者声明的序列号)
            if next_expected_sequence > self.highest_claimed_sequence.get() {
                // 如果下一个预期序列号超过了当前最高已声明序列号，
                // 说明生产者尚未声明它，无法再向前推进。
                break;
            }

            // 检查下一个预期的序列号是否已在 available_buffer 中标记为已发布。
            if self.available_buffer[next_expected_index].load(Ordering::Acquire) == 1 {
                // 如果已发布，尝试原子性地将 `cursor` 推进到 `next_expected_sequence`。
                match self.cursor.0.compare_exchange(
                    current_cursor,
                    next_expected_sequence,
                    Ordering::Release, // 成功时的内存顺序
                    Ordering::Relaxed, // 失败时的内存顺序
                ) {
                    Ok(_) => {
                        // 成功推进光标。
                        // 重置此槽位的 `available_buffer` 标志为 0，表示它已被光标“消费”，
                        // 并可供未来的生产者重新声明和发布。
                        self.available_buffer[next_expected_index].store(0, Ordering::Release);
                        current_cursor = next_expected_sequence; // 继续检查下一个序列
                    },
                    Err(actual_current) => {
                        // 另一个线程在此期间已推进了光标，
                        // 更新 `current_cursor` 并重新检查。
                        current_cursor = actual_current;
                        if current_cursor >= next_expected_sequence {
                            // 如果光标已经超过了我们当前正在检查的序列，
                            // 说明这个序列已经由其他线程处理并推进了光标。
                            // 重置 `available_buffer` 标志并继续检查更高的序列。
                             self.available_buffer[next_expected_index].store(0, Ordering::Release);
                             continue; // 继续检查更高的序列
                        }
                        // 发生竞争，自旋等待并重试
                        hint::spin_loop();
                    }
                }
            } else {
                // 下一个预期的序列号尚未发布，无法继续推进。
                break;
            }
        }
    }


    /// 获取所有 `gating_sequences` 中最低的序列号。
    /// 这代表了最慢的消费者已处理到的位置，用于生产者的反压检查。
    pub fn get_minimum_gating_sequence(&self) -> i64 {
        let gating_sequences_guard = self.gating_sequences.lock().unwrap();
        if gating_sequences_guard.is_empty() {
            // 如果没有消费者，则最小的门控序列是当前最高已发布序列，
            // 这样生产者就不会被消费者阻塞。
            return self.cursor.get(); 
        }

        let mut min_sequence = i64::MAX;
        for gating_sequence in gating_sequences_guard.iter() {
            min_sequence = min_sequence.min(gating_sequence.get());
        }
        min_sequence
    }


}



// --- 声明守卫 (ClaimedSequenceGuard) ---
// 此结构体确保已声明的序列号要么被显式发布，
// 要么在未发布的情况下被丢弃时发出警告/恐慌。
pub struct ClaimedSequenceGuard {
    sequence: i64,
    // 我们使用 AtomicI64 (作为布尔值) 来跟踪 `publish()` 是否已被调用。
    // 0: 未发布, 1: 已发布。
    published_flag: AtomicI64, 
    // 我们需要 `Arc` 到 `Sequencer` 以便在守卫被发布时调用其 `mark_sequence_as_published` 方法。
    sequencer: Arc<Sequencer>,
}
    
impl ClaimedSequenceGuard {
    fn new(sequence: i64, sequencer: Arc<Sequencer>) -> Self {
        ClaimedSequenceGuard {
            sequence,
            published_flag: AtomicI64::new(0), // 初始状态为未发布
            sequencer,
        }
    }
    
    /// 返回此守卫声明的序列号。
    pub fn sequence(&self) -> i64 {
        self.sequence
    }
    
    /// 显式发布已声明的序列号。
    /// 这会消耗守卫，从而防止 `drop` 被调用。
    pub fn publish(self) {
        // 将发布标志设置为 true。
        self.published_flag.store(1, Ordering::Release);
        // 通知序列协调器此序列号已发布。
        self.sequencer.mark_sequence_as_published(self.sequence);
        // `self` 在这里被消耗，因此 `drop` 不会被再次调用。
    }
}

// --- ClaimedSequenceGuard 的 Drop Trait 实现 ---
impl Drop for ClaimedSequenceGuard {
    fn drop(&mut self) {
        // 如果 `published_flag` 仍为 0，则表示 `publish()` 未被调用。
        if self.published_flag.load(Ordering::Acquire) == 0 {
            eprintln!(
                "CRITICAL ERROR: Claimed sequence {} was not published. Consumers will stall!",
                self.sequence
            );
            // 在实际应用场景中，您可能会在这里触发 `panic!` 以立即
            // 使应用程序崩溃并防止进一步的问题，或者记录到
            // 一个健壮的错误监控系统。
            // panic!("Producer failed to publish claimed sequence {}", self.sequence);
        }
    }
}