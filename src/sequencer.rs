// src/sequencer.rs

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI64, AtomicI8, Ordering};
use std::hint;

use crate::event::Event; 
use crate::ring_buffer::RingBuffer; 
use crate::wait_strategy::WaitStrategy; // <-- 确保 WaitStrategy trait 被引入

// --- Sequence Struct ---
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

    pub fn fetch_add(&self, delta: i64) -> i64 {
        self.0.fetch_add(delta, Ordering::SeqCst) + delta
    }
}

// --- Sequencer Struct ---
pub struct Sequencer {
    pub producer_cursor: Arc<Sequence>, 
    pub gating_sequences: Mutex<Vec<Arc<Sequence>>>,
    pub buffer_size: i64,
    index_mask: i64, 
    index_shift: i64, 
    available_buffer: Vec<AtomicI64>,
    wait_strategy: Arc<dyn WaitStrategy>, // <-- 确保此字段存在
}

impl Sequencer {
    // --- 核心修正点：确保 new 方法接受两个参数 ---
    pub fn new(buffer_size: usize, wait_strategy: Arc<dyn WaitStrategy>) -> Self { 
        assert!(buffer_size > 0, "Buffer size must be greater than 0");
        assert!(buffer_size.is_power_of_two(), "Buffer size must be a power of two");

        let capacity_i64 = buffer_size as i64;
        let index_shift = (buffer_size as f64).log2() as i64; 

        let available_buffer = (0..capacity_i64 as usize).map(|_| AtomicI64::new(-1)).collect();

        Sequencer {
            producer_cursor: Arc::new(Sequence::new(-1)), 
            gating_sequences: Mutex::new(Vec::new()),
            buffer_size: capacity_i64,
            index_mask: (capacity_i64 - 1), 
            index_shift,
            available_buffer,
            wait_strategy, // <-- 确保 wait_strategy 在这里被赋值
        }
    }

    pub fn add_gating_sequence(&self, sequence: Arc<Sequence>) {
        self.gating_sequences.lock().unwrap().push(sequence);
    }

    pub fn next(self: &Arc<Self>) -> ClaimedSequenceGuard {
        self.next_batch(1)
    }

    pub fn next_batch(self: &Arc<Self>, delta: i64) -> ClaimedSequenceGuard {
        if delta <= 0 || delta > self.buffer_size {
            panic!("Delta must be greater than 0 and less than or equal to buffer size.");
        }

        let mut current_claimed;
        let mut claimed_sequence_end;

        loop {
            current_claimed = self.producer_cursor.get(); 
            claimed_sequence_end = current_claimed + delta;

            let wrap_point = claimed_sequence_end - self.buffer_size;
            let min_gating_sequence = self.get_minimum_gating_sequence();

            if wrap_point > min_gating_sequence {
                hint::spin_loop(); 
                continue;
            }

            match self.producer_cursor.0.compare_exchange(
                current_claimed,
                claimed_sequence_end,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => {
                    hint::spin_loop();
                }
            }
        }
        
        ClaimedSequenceGuard::new(claimed_sequence_end, Arc::clone(self))
    }

    fn mark_sequence_as_published(self: &Arc<Self>, sequence: i64) {
        let index = (sequence & self.index_mask) as usize;
        //let flag_value = sequence >> self.index_shift; 
        self.available_buffer[index].store(sequence, Ordering::Release);

        self.wait_strategy.signal_all(); // <-- 确保此调用存在
    }

    pub fn get_minimum_gating_sequence(&self) -> i64 {
        let gating_sequences_guard = self.gating_sequences.lock().unwrap();
        if gating_sequences_guard.is_empty() {
            return self.producer_cursor.get(); 
        }

        let mut min_sequence = i64::MAX;
        for gating_sequence in gating_sequences_guard.iter() {
            min_sequence = min_sequence.min(gating_sequence.get());
        }
        min_sequence
    }

    pub fn get_highest_available_sequence(&self, consumer_sequence: i64) -> i64 { 
        let highest_claimed_sequence = self.producer_cursor.get(); 
        let mut available_sequence = consumer_sequence + 1; 

        while available_sequence <= highest_claimed_sequence {
            let index = (available_sequence & self.index_mask) as usize;
            //let expected_flag = available_sequence >> self.index_shift; 
            let stored_sequence = self.available_buffer[index].load(Ordering::Acquire);
            //if self.available_buffer[index].load(Ordering::Acquire) == expected_flag {
            if stored_sequence >= available_sequence {
                available_sequence += 1; 
            } else {
                return available_sequence - 1; 
            }
        }
        highest_claimed_sequence
    }
}

// --- ClaimedSequenceGuard Struct ---
pub struct ClaimedSequenceGuard {
    sequence: i64,
    published_flag: AtomicI64, 
    sequencer: Arc<Sequencer>, 
}

impl ClaimedSequenceGuard {
    fn new(sequence: i64, sequencer: Arc<Sequencer>) -> Self {
        ClaimedSequenceGuard {
            sequence,
            published_flag: AtomicI64::new(0),
            sequencer,
        }
    }

    pub fn sequence(&self) -> i64 {
        self.sequence
    }

    pub fn publish(self) {
        self.published_flag.store(1, Ordering::Release);
        self.sequencer.mark_sequence_as_published(self.sequence);
    }
}

impl Drop for ClaimedSequenceGuard {
    fn drop(&mut self) {
        if self.published_flag.load(Ordering::Acquire) == 0 {
            eprintln!(
                "CRITICAL ERROR: Claimed sequence {} was not published. Consumers will stall!",
                self.sequence
            );
        }
    }
}
