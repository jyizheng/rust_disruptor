// src/main.rs

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::collections::HashSet;
use std::sync::Mutex; // Required for Mutex used with Vec

// Declare modules
mod event;         
mod sequencer;     
mod ring_buffer;   
mod wait_strategy; 
mod producer;      
mod consumer;      
mod disruptor;     

// Import necessary types
use crate::event::MyEvent;
use crate::wait_strategy::BusySpinWaitStrategy; 
use crate::disruptor::Disruptor;
use crate::sequencer::{Sequencer, Sequence, ClaimedSequenceGuard}; 
use crate::ring_buffer::RingBuffer; 
use crate::producer::Producer; 
use crate::consumer::Consumer; 

// --- Producer Task Function (Already moved here) ---
fn producer_task(producer: Producer<MyEvent>, id: usize, fail_on_sequence: Option<u64>) {
    println!("[生产者 {}] 生产者线程启动。", id);
    for i in 0..10 { 
        let current_claimed_seq_before_next = producer.sequencer.producer_cursor.get();
        println!("[生产者 {}] 尝试声明序列号 {}", id, current_claimed_seq_before_next + 1);

        let claim_guard = producer.next(); 
        let sequence = claim_guard.sequence();

        unsafe {
            let event_entry = producer.get_mut(sequence); 
            event_entry.value = (i + (id - 1) * 10) as u64; 
            println!(
                "[生产者 {}] 写入事件 {{ value: {} }} 到序列号 {}",
                id,
                event_entry.value,
                sequence
            );
        }

        if let Some(fail_seq) = fail_on_sequence {
            if sequence == fail_seq as i64 {
                println!("[生产者 {}] 模拟失败：不发布序列号 {}", id, sequence);
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        }

        claim_guard.publish(); 
        println!("[生产者 {}] 成功发布序列号 {}", id, sequence);

        thread::sleep(Duration::from_millis(50));
    }
    println!("[生产者 {}] 生产者线程完成。", id);
}


// --- Consumer Task Function (Modified to accept data collector) ---
fn consumer_task(
    consumer: Consumer<MyEvent, BusySpinWaitStrategy>, 
    processed_values_clone: Arc<Mutex<Vec<u64>>>, // <-- Added parameter for data collection
    id: usize, 
    total_events_to_consume: u64,
) {
    println!("[消费者 {}] 消费者线程启动。", id);
    let mut count = 0; 
    loop {
        // process_event 内部会调用提供的闭包
        let result = consumer.process_event(|event: &MyEvent| {
            // 使用传入的 Arc<Mutex> 来收集数据
            println!("[消费者 {}] 处理事件: {:?}", id, event);
            processed_values_clone.lock().unwrap().push(event.value); 
        });

        if let Some(_) = result { 
            count += 1;
            if count >= total_events_to_consume { 
                println!("[消费者 {}] 处理了所有 {} 个事件。退出。", id, total_events_to_consume);
                break;
            }
        } else {
            thread::yield_now(); 
        }
    }
    println!("[消费者 {}] 消费者线程完成。", id);
}

// --- Main Function ---
fn main() {
    println!("启动 Rust 中的 LMAX Disruptor 模拟示例...");

    const BUFFER_SIZE: usize = 16;
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(BUFFER_SIZE, BusySpinWaitStrategy::default()); 

    let producer1 = disruptor.create_producer(); 
    let producer2 = disruptor.create_producer();

    let consumer1 = disruptor.create_consumer(None); 
    let consumer2 = disruptor.create_consumer(None); 

    let processed_values_c1 = Arc::new(Mutex::new(Vec::<u64>::new())); 
    let processed_values_c1_clone = Arc::clone(&processed_values_c1);

    let processed_values_c2 = Arc::new(Mutex::new(Vec::<u64>::new())); 
    let processed_values_c2_clone = Arc::clone(&processed_values_c2);

    const NUM_EVENTS_PER_PRODUCER: usize = 10;
    const TOTAL_EVENTS: usize = NUM_EVENTS_PER_PRODUCER * 2; 

    // --- 修正点：调用 consumer_task 函数启动消费者线程 ---
    let consumer1_handle = thread::spawn(move || {
        consumer_task(consumer1, processed_values_c1_clone, 1, TOTAL_EVENTS as u64); 
    });

    let consumer2_handle = thread::spawn(move || {
        consumer_task(consumer2, processed_values_c2_clone, 2, TOTAL_EVENTS as u64); 
    });

    // 启动生产者线程
    let producer1_handle = thread::spawn(move || {
        producer_task(producer1, 1, None); 
    });
    let producer2_handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        producer_task(producer2, 2, None); 
    });

    // 等待所有线程完成
    producer1_handle.join().expect("生产者 1 线程 panic");
    producer2_handle.join().expect("生产者 2 线程 panic");
    consumer1_handle.join().expect("消费者 1 线程 panic");
    consumer2_handle.join().expect("消费者 2 线程 panic");

    // 合并并验证所有消费者的数据
    let mut combined_final_values: Vec<u64> = Vec::new();
    combined_final_values.extend_from_slice(&processed_values_c1.lock().unwrap());
    combined_final_values.extend_from_slice(&processed_values_c2.lock().unwrap());
    
    combined_final_values.sort_unstable(); 

    println!("\n所有事件已处理。最终收集到的值 (排序后，可能包含来自多个消费者的重复)：{:?}", combined_final_values);

    // 验证输出：检查唯一性，并确保所有事件都被处理
    let mut expected_values_set: HashSet<u64> = HashSet::new();
    for i in 0..NUM_EVENTS_PER_PRODUCER {
        expected_values_set.insert(i as u64); 
        expected_values_set.insert((i + NUM_EVENTS_PER_PRODUCER) as u64); 
    }

    let mut actual_unique_values_set = HashSet::new();
    for &val in combined_final_values.iter() {
        actual_unique_values_set.insert(val);
    }

    assert_eq!(actual_unique_values_set.len(), TOTAL_EVENTS, "处理的唯一事件数量不正确。");
    assert_eq!(actual_unique_values_set, expected_values_set, "处理的值集合与期望值集合不匹配。");
    println!("断言通过：处理的值与期望值匹配且是唯一的。");

    println!("Rust Disruptor 多生产者多消费者示例完成。");
}
