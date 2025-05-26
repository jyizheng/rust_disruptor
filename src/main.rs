// src/main.rs

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::collections::HashSet;
use std::sync::Mutex;

mod event;         
mod sequencer;     
mod ring_buffer;   
mod wait_strategy; 
mod producer;      
mod consumer;      
mod disruptor;     

use crate::event::MyEvent;
use crate::wait_strategy::BusySpinWaitStrategy; 
use crate::disruptor::Disruptor;
use crate::sequencer::{Sequencer, Sequence, ClaimedSequenceGuard}; 
use crate::ring_buffer::RingBuffer; 
use crate::producer::Producer; 
use crate::consumer::Consumer; 

// --- Producer Task Function ---
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
        }

        if let Some(fail_seq) = fail_on_sequence {
            if sequence == fail_seq as i64 {
                println!("[生产者 {}] 模拟失败：不发布序列号 {}", id, sequence);
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        }

        claim_guard.publish(); 
        thread::sleep(Duration::from_millis(50));
    }
    println!("[生产者 {}] 生产者线程完成。", id);
}


// --- Consumer Task Function ---
fn consumer_task(
    consumer: Consumer<MyEvent, BusySpinWaitStrategy>, 
    processed_values_clone: Arc<Mutex<Vec<u64>>>, 
    id: usize, 
    total_events_to_consume: u64,
) {
    println!("[消费者 {}] 消费者线程启动。", id);
    let mut count = 0; 
    loop {
        let result = consumer.process_event(|event: &MyEvent| {
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

    // --- 创建独立的消费者 C1 和 C2 (不依赖其他消费者) ---
    let consumer1 = disruptor.create_consumer(vec![]); // 空 Vec 表示没有依赖
    let consumer2 = disruptor.create_consumer(vec![]); 

    // --- 修正点：创建消费者 C3，依赖于 C1 和 C2 ---
    let consumer3 = disruptor.create_consumer(vec![
        // 克隆 C1 的序列号
        Arc::clone(&consumer1.sequence), 
        // 克隆 C2 的序列号
        Arc::clone(&consumer2.sequence), 
    ]);

    // 为每个消费者准备数据收集器
    let processed_values_c1 = Arc::new(Mutex::new(Vec::<u64>::new())); 
    let processed_values_c1_clone = Arc::clone(&processed_values_c1);

    let processed_values_c2 = Arc::new(Mutex::new(Vec::<u64>::new())); 
    let processed_values_c2_clone = Arc::clone(&processed_values_c2);

    let processed_values_c3 = Arc::new(Mutex::new(Vec::<u64>::new())); // C3 的收集器
    let processed_values_c3_clone = Arc::clone(&processed_values_c3);


    const NUM_EVENTS_PER_PRODUCER: usize = 10;
    const TOTAL_EVENTS: usize = NUM_EVENTS_PER_PRODUCER * 2; 

    // --- 启动消费者线程 ---
    let consumer1_handle = thread::spawn(move || {
        consumer_task(consumer1, processed_values_c1_clone, 1, TOTAL_EVENTS as u64); 
    });

    let consumer2_handle = thread::spawn(move || {
        consumer_task(consumer2, processed_values_c2_clone, 2, TOTAL_EVENTS as u64); 
    });

    // --- 修正点：启动消费者 C3 线程 ---
    let consumer3_handle = thread::spawn(move || {
        consumer_task(consumer3, processed_values_c3_clone, 3, TOTAL_EVENTS as u64); 
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
    consumer3_handle.join().expect("消费者 3 线程 panic"); // 等待 C3

    // --- 修正点：合并并验证所有消费者的数据 ---
    let mut combined_final_values: Vec<u64> = Vec::new();
    combined_final_values.extend_from_slice(&processed_values_c1.lock().unwrap());
    combined_final_values.extend_from_slice(&processed_values_c2.lock().unwrap());
    combined_final_values.extend_from_slice(&processed_values_c3.lock().unwrap()); // 包含 C3 的数据
    
    combined_final_values.sort_unstable(); 

    println!("\n所有事件已处理。最终收集到的值 (排序后，可能包含来自多个消费者的重复)：{:?}", combined_final_values);

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
