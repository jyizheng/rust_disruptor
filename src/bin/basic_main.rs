// src/bin/basic_main.rs

// Replace `your_crate_name` with the actual name of your crate from Cargo.toml
// e.g., if your crate is named "rust_disruptor", use `rust_disruptor::event::MyEvent;`
use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::producer::Producer;
use rust_disruptor::sequencer::{ProducerMode, Sequence}; // Assuming Sequence might be needed for Arc clones
use rust_disruptor::wait_strategy::BusySpinWaitStrategy;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// --- Producer Task Function (Copied from original main.rs) ---
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

// --- Consumer Task Function (Copied from original main.rs) ---
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

fn main() {
    println!("\n--- 运行基础多生产者多消费者示例 (单独二进制) ---");

    const BUFFER_SIZE_BASIC: usize = 16;
    let mut disruptor_basic = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        BUFFER_SIZE_BASIC,
        BusySpinWaitStrategy::default(),
        ProducerMode::Multi,
    );

    let producer1_basic = disruptor_basic.create_producer();
    let producer2_basic = disruptor_basic.create_producer();

    let consumer1_basic = disruptor_basic.create_consumer(vec![]);
    let consumer2_basic = disruptor_basic.create_consumer(vec![]);

    let consumer3_basic = disruptor_basic.create_consumer(vec![
        Arc::clone(&consumer1_basic.sequence),
        Arc::clone(&consumer2_basic.sequence),
    ]);

    let processed_values_c1_basic = Arc::new(Mutex::new(Vec::<u64>::new()));
    let processed_values_c1_clone_basic = Arc::clone(&processed_values_c1_basic);

    let processed_values_c2_basic = Arc::new(Mutex::new(Vec::<u64>::new()));
    let processed_values_c2_clone_basic = Arc::clone(&processed_values_c2_basic);

    let processed_values_c3_basic = Arc::new(Mutex::new(Vec::<u64>::new()));
    let processed_values_c3_clone_basic = Arc::clone(&processed_values_c3_basic);

    const NUM_EVENTS_PER_PRODUCER_BASIC: usize = 10;
    const TOTAL_EVENTS_BASIC: usize = NUM_EVENTS_PER_PRODUCER_BASIC * 2;

    let consumer1_handle_basic = thread::spawn(move || {
        consumer_task(consumer1_basic, processed_values_c1_clone_basic, 1, TOTAL_EVENTS_BASIC as u64);
    });

    let consumer2_handle_basic = thread::spawn(move || {
        consumer_task(consumer2_basic, processed_values_c2_clone_basic, 2, TOTAL_EVENTS_BASIC as u64);
    });

    let consumer3_handle_basic = thread::spawn(move || {
        consumer_task(consumer3_basic, processed_values_c3_clone_basic, 3, TOTAL_EVENTS_BASIC as u64);
    });

    let producer1_handle_basic = thread::spawn(move || {
        producer_task(producer1_basic, 1, None);
    });
    let producer2_handle_basic = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        producer_task(producer2_basic, 2, None);
    });

    producer1_handle_basic.join().expect("生产者 1 线程 panic");
    producer2_handle_basic.join().expect("生产者 2 线程 panic");
    consumer1_handle_basic.join().expect("消费者 1 线程 panic");
    consumer2_handle_basic.join().expect("消费者 2 线程 panic");
    consumer3_handle_basic.join().expect("消费者 3 线程 panic");

    let mut combined_final_values_basic: Vec<u64> = Vec::new();
    combined_final_values_basic.extend_from_slice(&processed_values_c1_basic.lock().unwrap());
    combined_final_values_basic.extend_from_slice(&processed_values_c2_basic.lock().unwrap());
    combined_final_values_basic.extend_from_slice(&processed_values_c3_basic.lock().unwrap());

    combined_final_values_basic.sort_unstable();

    println!("\n所有事件已处理。最终收集到的值 (排序后，可能包含来自多个消费者的重复)：{:?}", combined_final_values_basic);

    let mut expected_values_set_basic: HashSet<u64> = HashSet::new();
    for i in 0..NUM_EVENTS_PER_PRODUCER_BASIC {
        expected_values_set_basic.insert(i as u64);
        expected_values_set_basic.insert((i + NUM_EVENTS_PER_PRODUCER_BASIC) as u64);
    }

    let mut actual_unique_values_set_basic = HashSet::new();
    for &val in combined_final_values_basic.iter() {
        actual_unique_values_set_basic.insert(val);
    }

    assert_eq!(actual_unique_values_set_basic.len(), TOTAL_EVENTS_BASIC, "处理的唯一事件数量不正确。");
    assert_eq!(actual_unique_values_set_basic, expected_values_set_basic, "处理的值集合与期望值集合不匹配。");
    println!("断言通过：处理的值与期望值匹配且是唯一的。");

    println!("Rust Disruptor 基础多生产者多消费者示例完成。");
}

