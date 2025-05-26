use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::mpsc;

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
use crate::wait_strategy::{BusySpinWaitStrategy, YieldingWaitStrategy, BlockingWaitStrategy, PhasedBackoffWaitStrategy}; // 引入所有等待策略
use crate::disruptor::Disruptor;
use crate::sequencer::{Sequencer, Sequence, ClaimedSequenceGuard}; 
use crate::ring_buffer::RingBuffer; 
use crate::producer::Producer; 
use crate::consumer::Consumer;
use crate::wait_strategy::WaitStrategy;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

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
    consumer: Consumer<MyEvent, BusySpinWaitStrategy>, // 这里的 WaitStrategy 类型应该与 Disruptor 实例一致
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

// --- Basic Multi-Producer Multi-Consumer Test Function ---
fn basic_test() {
    println!("\n--- 运行基础多生产者多消费者示例 ---");

    const BUFFER_SIZE_BASIC: usize = 16; 
    // WaitStrategy 类型现在是泛型参数，需要传递实例
    let mut disruptor_basic = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(BUFFER_SIZE_BASIC, BusySpinWaitStrategy::default()); 

    let producer1_basic = disruptor_basic.create_producer(); 
    let producer2_basic = disruptor_basic.create_producer();

    // 创建消费者时，传递 Vec<Arc<Sequence>> 作为依赖 (空 Vec 表示无依赖)
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


// --- Performance Test Function: OneToOneSequencedThroughputTest ---
fn run_one_to_one_throughput_test() {
    println!("\n--- 运行一对一有序吞吐量测试 ---");

    const BUFFER_SIZE_PERF: usize = 1024 * 64; // 65536
    const ITERATIONS_PERF: u64 = 1_000_000; // Adjust this to test the stall scenario
    // const ITERATIONS_PERF: u64 = 100_000_000; // Original high-throughput test value
    
    let expected_sum_perf: u64 = (ITERATIONS_PERF * (ITERATIONS_PERF - 1)) / 2;

    // --- 修正点：使用 BlockingWaitStrategy 来验证活锁问题 ---
    let mut disruptor_perf = Disruptor::<MyEvent, BlockingWaitStrategy>::new(BUFFER_SIZE_PERF, BlockingWaitStrategy::default());
    let producer_perf = disruptor_perf.create_producer(); 
    // 单个消费者，无依赖
    let consumer_perf = disruptor_perf.create_consumer(vec![]); 

    let accumulated_sum_perf = Arc::new(AtomicU64::new(0)); 
    let (tx_perf, rx_perf) = mpsc::channel(); 

    let consumer_thread_sum_clone_perf = Arc::clone(&accumulated_sum_perf);
    let consumer_thread_tx_clone_perf = tx_perf;
    let consumer_handle_perf = thread::spawn(move || {
        println!("[性能测试消费者] 消费者线程启动。");
        let mut processed_count_perf = 0;
        let mut last_sequence_processed_perf = -1;

        loop {
            let highest_available_perf = consumer_perf.sequencer.get_highest_available_sequence(last_sequence_processed_perf);

            if highest_available_perf > last_sequence_processed_perf {
                let next_sequence_to_consume_perf = last_sequence_processed_perf + 1;

                unsafe {
                    let event_perf = consumer_perf.ring_buffer.get(next_sequence_to_consume_perf);
                    consumer_thread_sum_clone_perf.fetch_add(event_perf.value, Ordering::SeqCst);
                    if (event_perf.value % 100 == 0) {
                       println!("[性能测试消费者] 消费序列号: {}, 值: {}", next_sequence_to_consume_perf, event_perf.value); // Verbose logging
                    }
                }
                last_sequence_processed_perf = next_sequence_to_consume_perf;
                consumer_perf.sequence.set(last_sequence_processed_perf);
                processed_count_perf += 1;

                if processed_count_perf >= ITERATIONS_PERF {
                    println!("[性能测试消费者] 处理了所有 {} 个事件。退出。", ITERATIONS_PERF);
                    break; 
                }
            } else {
                consumer_perf.wait_strategy.wait_for(
                    last_sequence_processed_perf + 1, 
                    Arc::clone(&consumer_perf.sequencer), // Correctly passing Arc<Sequencer>
                    &consumer_perf.gating_sequences_for_wait, 
                    consumer_perf.sequence.clone(), 
                );
            }
        }
        consumer_thread_tx_clone_perf.send(()).expect("Could not send completion signal");
        println!("[性能测试消费者] 消费者线程完成。");
    });

    println!("[性能测试生产者] 生产者线程启动。");
    let start_time_perf = Instant::now();

    for i in 0..ITERATIONS_PERF {
        let claim_guard_perf = producer_perf.next();
        let sequence_perf = claim_guard_perf.sequence();
        unsafe {
            let event_perf = producer_perf.get_mut(sequence_perf);
            event_perf.value = i;
            if (event_perf.value % 100 == 0) {
                println!("[性能测试生产者] 发布序列号: {}", sequence_perf); // Verbose logging
            }
        }
        claim_guard_perf.publish();
    }
    println!("[性能测试生产者] 生产者线程完成。");

    rx_perf.recv().expect("Failed to receive completion signal from consumer");
    let end_time_perf = Instant::now();

    let elapsed_ms = end_time_perf.duration_since(start_time_perf).as_millis() as u64;
    let ops_per_second = (ITERATIONS_PERF * 1000) / elapsed_ms;

    println!("\n--- 性能测试结果 ---");
    println!("总迭代次数 (ITERATIONS): {}", ITERATIONS_PERF);
    println!("缓冲区大小 (BUFFER_SIZE): {}", BUFFER_SIZE_PERF);
    println!("耗时: {} ms", elapsed_ms);
    println!("吞吐量: {} ops/sec", ops_per_second);

    let final_sum = accumulated_sum_perf.load(Ordering::SeqCst);
    println!("最终累加和: {}", final_sum);
    println!("期望累加和: {}", expected_sum_perf);

    assert_eq!(final_sum, expected_sum_perf, "累加结果不匹配！");
    println!("断言通过：累加结果正确。");

    consumer_handle_perf.join().expect("消费者线程 panic");
}


// --- Main Function ---
fn main() {
    // 运行基础多生产者多消费者示例
    basic_test();

    // 运行一对一有序吞吐量性能测试
    run_one_to_one_throughput_test();
}
