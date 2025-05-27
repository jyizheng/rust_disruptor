// src/bin/perf_main.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::sequencer::ProducerMode;
use rust_disruptor::wait_strategy::BusySpinWaitStrategy;
use rust_disruptor::wait_strategy::WaitStrategy;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Instant;

// --- Consumer Task Function  ---
fn consumer_task_perf_test(
    consumer: Consumer<MyEvent, BusySpinWaitStrategy>,
    accumulated_sum_perf: Arc<AtomicU64>,
    iterations_to_process: u64,
    tx_perf: mpsc::Sender<()>,
) {
    println!("[性能测试消费者] 消费者线程启动。");
    let mut processed_count_perf = 0;

    while processed_count_perf < iterations_to_process {
        let next_sequence_to_try = consumer.sequence.get() + 1;
        let highest_available_by_wait_strategy = consumer.wait_strategy.wait_for(
            next_sequence_to_try,
            Arc::clone(&consumer.sequencer),
            &consumer.gating_sequences_for_wait,
            Arc::clone(&consumer.sequence),
        );

        if highest_available_by_wait_strategy >= next_sequence_to_try {
            for seq_to_process in next_sequence_to_try..=highest_available_by_wait_strategy {
                if processed_count_perf >= iterations_to_process {
                    break;
                }
                // Add unsafe block because RingBuffer::get() is unsafe
                unsafe {
                    let event_perf = consumer.ring_buffer.get(seq_to_process);
                    accumulated_sum_perf.fetch_add(event_perf.value, Ordering::Relaxed);
                }
                processed_count_perf += 1;
            }
            consumer.sequence.set(highest_available_by_wait_strategy);
        }
    }

    println!("[性能测试消费者] 处理了所有 {} 个事件。退出。", iterations_to_process);
    if tx_perf.send(()).is_err() {
        eprintln!("[性能测试消费者] 无法发送完成信号。接收端可能已关闭。");
    }
    println!("[性能测试消费者] 消费者线程完成。");
}

fn main() {
    println!("\n--- 运行一对一有序吞吐量测试 (单独二进制) ---");

    const BUFFER_SIZE_PERF: usize = 65536;
    const ITERATIONS_PERF: u64 = 100_000_000;
    let expected_sum_perf: u64 = (ITERATIONS_PERF * (ITERATIONS_PERF - 1)) / 2;

    let mut disruptor_perf = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        BUFFER_SIZE_PERF,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    let producer_perf = disruptor_perf.create_producer();
    let consumer_perf = disruptor_perf.create_consumer(vec![]);

    let accumulated_sum_perf = Arc::new(AtomicU64::new(0));
    let (tx_perf, rx_perf) = mpsc::channel();

    let consumer_thread_sum_clone_perf = Arc::clone(&accumulated_sum_perf);
    let consumer_thread_tx_clone_perf = tx_perf;

    let consumer_handle_perf = thread::spawn(move || {
        consumer_task_perf_test(
            consumer_perf,
            consumer_thread_sum_clone_perf,
            ITERATIONS_PERF,
            consumer_thread_tx_clone_perf,
        );
    });

    println!("[性能测试生产者] 生产者线程启动。");
    let start_time_perf = Instant::now();

    for i in 0..ITERATIONS_PERF {
        let claim_guard_perf = producer_perf.next();
        let sequence_perf = claim_guard_perf.sequence();
        unsafe {
            let event_perf = producer_perf.get_mut(sequence_perf);
            event_perf.value = i;
        }
        claim_guard_perf.publish();
    }
    println!("[性能测试生产者] 生产者线程完成。");

    rx_perf.recv().expect("Failed to receive completion signal from consumer");
    let end_time_perf = Instant::now();

    let elapsed_ms = end_time_perf.duration_since(start_time_perf).as_millis() as u64;

    println!("\n--- 性能测试结果 ---");
    println!("总迭代次数 (ITERATIONS): {}", ITERATIONS_PERF);
    println!("缓冲区大小 (BUFFER_SIZE): {}", BUFFER_SIZE_PERF);
    println!("耗时: {} ms", elapsed_ms);

    if elapsed_ms > 0 {
        let ops_per_second = (ITERATIONS_PERF * 1000) / elapsed_ms;
        println!("吞吐量: {} ops/sec", ops_per_second);
    } else {
        println!("吞吐量: N/A (耗时为0或过短)");
    }

    let final_sum = accumulated_sum_perf.load(Ordering::SeqCst);
    println!("最终累加和: {}", final_sum);
    println!("期望累加和: {}", expected_sum_perf);

    assert_eq!(final_sum, expected_sum_perf, "累加结果不匹配！");
    println!("断言通过：累加结果正确。");

    consumer_handle_perf.join().expect("消费者线程 panic");
}
