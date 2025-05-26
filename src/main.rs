// src/main.rs

mod event;         
mod sequencer;     
mod ring_buffer;   
mod wait_strategy; 
mod producer;      
mod consumer;      
mod disruptor;     

use event::MyEvent;
use wait_strategy::BusySpinWaitStrategy;
use disruptor::Disruptor;
use std::thread;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::collections::HashSet;

fn main() {
    println!("启动 Rust Disruptor 多生产者多消费者示例...");

    const BUFFER_SIZE: usize = 16;
    // 初始化 Disruptor，指定事件类型和等待策略
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(BUFFER_SIZE, BusySpinWaitStrategy);

    // 创建多个生产者实例
    let producer1 = disruptor.create_producer(); 
    let producer2 = disruptor.create_producer();

    // --- 修正点：创建多个消费者实例 ---
    let consumer1 = disruptor.create_consumer(None); // 消费者 1，目前没有依赖
    let consumer2 = disruptor.create_consumer(None); // 消费者 2，目前没有依赖

    // --- 修正点：为每个消费者准备独立的数据收集器 ---
    let processed_values_c1 = Arc::new(Mutex::new(Vec::<u64>::new())); 
    let processed_values_c1_clone = Arc::clone(&processed_values_c1);

    let processed_values_c2 = Arc::new(Mutex::new(Vec::<u64>::new())); 
    let processed_values_c2_clone = Arc::clone(&processed_values_c2);

    // 定义每个生产者将生产的事件数量
    const NUM_EVENTS_PER_PRODUCER: usize = 10;
    const TOTAL_EVENTS: usize = NUM_EVENTS_PER_PRODUCER * 2; // 总事件数 (2 个生产者)

    // --- 修正点：启动第一个消费者线程 ---
    let consumer1_handle = thread::spawn(move || {
        println!("[消费者 1] 消费者线程启动。");
        let mut count = 0;

        loop {
            // 消费者处理事件的逻辑，将事件值收集到自己的 Vec 中
            let result = consumer1.process_event(|event: &MyEvent| {
                processed_values_c1_clone.lock().unwrap().push(event.value);
            });

            if let Some(_) = result { // 如果处理了事件
                count += 1;
                // 当处理的事件数量达到总事件数时，消费者完成任务
                if count >= TOTAL_EVENTS { 
                    println!("[消费者 1] 处理了所有 {} 个事件。退出。", TOTAL_EVENTS);
                    break;
                }
            } else {
                // 没有新事件可用，让出 CPU
                thread::yield_now(); 
            }
        }
        println!("[消费者 1] 消费者线程完成。");
    });

    // --- 修正点：启动第二个消费者线程 ---
    let consumer2_handle = thread::spawn(move || {
        println!("[消费者 2] 消费者线程启动。");
        let mut count = 0;

        loop {
            // 消费者处理事件的逻辑
            let result = consumer2.process_event(|event: &MyEvent| {
                processed_values_c2_clone.lock().unwrap().push(event.value);
            });

            if let Some(_) = result { // 如果处理了事件
                count += 1;
                if count >= TOTAL_EVENTS { 
                    println!("[消费者 2] 处理了所有 {} 个事件。退出。", TOTAL_EVENTS);
                    break;
                }
            } else {
                thread::yield_now(); 
            }
        }
        println!("[消费者 2] 消费者线程完成。");
    });

    // 启动生产者 1 线程
    let producer1_handle = thread::spawn(move || {
        println!("[生产者 1] 生产者线程启动。");
        for i in 0..NUM_EVENTS_PER_PRODUCER {
            let claim_guard = producer1.next(); 
            let sequence = claim_guard.sequence();
            
            unsafe {
                let event = producer1.get_mut(sequence); 
                event.value = i as u64; 
            }
            claim_guard.publish(); 
            thread::sleep(Duration::from_millis(50));
        }
        println!("[生产者 1] 生产者线程完成。");
    });

    // 启动生产者 2 线程
    let producer2_handle = thread::spawn(move || {
        println!("[生产者 2] 生产者线程启动。");
        for i in 0..NUM_EVENTS_PER_PRODUCER {
            let claim_guard = producer2.next();
            let sequence = claim_guard.sequence();
            
            unsafe {
                let event = producer2.get_mut(sequence);
                event.value = (i + NUM_EVENTS_PER_PRODUCER) as u64; 
            }
            claim_guard.publish(); 
            thread::sleep(Duration::from_millis(70));
        }
        println!("[生产者 2] 生产者线程完成。");
    });


    // 等待所有线程完成
    producer1_handle.join().expect("生产者 1 线程 panic");
    producer2_handle.join().expect("生产者 2 线程 panic");
    // --- 修正点：等待所有消费者线程完成 ---
    consumer1_handle.join().expect("消费者 1 线程 panic");
    consumer2_handle.join().expect("消费者 2 线程 panic");

    // --- 修正点：合并并验证所有消费者的数据 ---
    let mut combined_final_values: Vec<u64> = Vec::new();
    // 从消费者 1 的收集器中获取数据
    combined_final_values.extend_from_slice(&processed_values_c1.lock().unwrap());
    // 从消费者 2 的收集器中获取数据
    combined_final_values.extend_from_slice(&processed_values_c2.lock().unwrap());
    
    // 对合并后的数据进行排序（方便观察，顺序不保证）
    combined_final_values.sort_unstable(); 

    println!("\n所有事件已处理。最终收集到的值 (排序后，可能包含来自多个消费者的重复)：{:?}", combined_final_values);

    // 验证输出：检查唯一性，并确保所有事件都被处理
    let mut expected_values_set: HashSet<u64> = HashSet::new();
    for i in 0..NUM_EVENTS_PER_PRODUCER {
        expected_values_set.insert(i as u64); // 来自生产者 1 的事件
        expected_values_set.insert((i + NUM_EVENTS_PER_PRODUCER) as u64); // 来自生产者 2 的事件
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