// src/bin/consumer_batch_main.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::producer::Producer;
use rust_disruptor::sequencer::ProducerMode;
// Updated imports to include BlockingWaitStrategy
use rust_disruptor::wait_strategy::{BlockingWaitStrategy, BusySpinWaitStrategy, WaitStrategy, YieldingWaitStrategy};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex}; // Added Mutex
use std::thread;
use std::time::{Duration, Instant};

// --- Producer Task Function ---
fn producer_task(
    producer: Producer<MyEvent>,
    iterations: u64,
) {
    // println!("[Producer] Starting. Will produce {} events.", iterations);
    for i in 0..iterations {
        let claim_guard = producer.next();
        let sequence = claim_guard.sequence();
        unsafe {
            let event_entry = producer.get_mut(sequence);
            event_entry.value = i; // Values from 0 to iterations-1
        }
        claim_guard.publish();
    }
    // println!("[Producer] Finished producing {} events.", iterations);
}

// --- Consumer Task Function (using batch processing for perf) ---
fn consumer_batch_task<W: WaitStrategy + Send + Sync + 'static>(
    consumer: Consumer<MyEvent, W>,
    accumulated_sum: Arc<AtomicU64>,
    batch_sizes_log: Arc<Mutex<Vec<u64>>>,
    iterations_to_process: u64,
    completion_tx: mpsc::Sender<()>,
) {
    // println!("[Consumer] Starting. Expecting {} events.", iterations_to_process);
    let mut consumed_count = 0;
    let mut local_sum: u64 = 0;
    let mut local_batch_sizes = Vec::with_capacity((iterations_to_process / 100).max(1) as usize);

    while consumed_count < iterations_to_process {
        let batch_size = consumer.process_event_batch(|event: &MyEvent, _sequence: i64, _end_of_batch: bool| {
            local_sum += event.value;
        });

        if batch_size > 0 {
            consumed_count += batch_size;
            local_batch_sizes.push(batch_size);
        } else {
            // Rely on WaitStrategy
        }
    }

    accumulated_sum.fetch_add(local_sum, Ordering::Relaxed);
    if let Ok(mut guard) = batch_sizes_log.lock() {
        guard.extend(local_batch_sizes);
    }
    completion_tx.send(()).expect("Consumer: Could not send completion signal");
    // println!("[Consumer] Finished. Processed {} events. Local sum: {}", consumed_count, local_sum);
}


fn run_spsc_batch_throughput_test<W: WaitStrategy + Clone + Send + Sync + Default + 'static>(
    strategy_name: &str,
    buffer_size: usize,
    iterations: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "\n--- Running SPSC Batch Throughput Test for: {} ---",
        strategy_name
    );

    let wait_strategy_instance = W::default();

    let mut disruptor = Disruptor::<MyEvent, W>::new(
        buffer_size,
        wait_strategy_instance,
        ProducerMode::Single,
    );

    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);

    let accumulated_sum = Arc::new(AtomicU64::new(0));
    let batch_sizes_log = Arc::new(Mutex::new(Vec::new()));
    let (completion_tx, completion_rx) = mpsc::channel();

    let consumer_sum_clone = Arc::clone(&accumulated_sum);
    let consumer_batch_log_clone = Arc::clone(&batch_sizes_log);

    let consumer_thread = thread::Builder::new()
        .name(format!("spsc-batch-consumer-{}", strategy_name))
        .spawn(move || {
            consumer_batch_task(
                consumer,
                consumer_sum_clone,
                consumer_batch_log_clone,
                iterations,
                completion_tx,
            );
        })?;

    thread::sleep(Duration::from_millis(100));

    let producer_thread = thread::Builder::new()
        .name(format!("spsc-batch-producer-{}", strategy_name))
        .spawn(move || {
            producer_task(producer, iterations);
        })?;

    let start_time = Instant::now();

    producer_thread.join().expect("Producer thread panicked");

    match completion_rx.recv_timeout(Duration::from_secs(180)) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("[Test-{}] Timeout or error waiting for consumer completion: {:?}. Processed sum: {}",
                strategy_name, e, accumulated_sum.load(Ordering::Relaxed));
            return Err(Box::new(e));
        }
    }
    let end_time = Instant::now();

    consumer_thread.join().expect("Consumer thread panicked");

    let elapsed_duration = end_time.duration_since(start_time);
    let elapsed_ms = elapsed_duration.as_millis();
    let ops_per_second = if elapsed_ms > 0 {
        (iterations * 1000) / elapsed_ms as u64
    } else {
        iterations 
    };

    let final_sum = accumulated_sum.load(Ordering::SeqCst);
    let expected_sum = if iterations > 0 { (iterations * (iterations - 1)) / 2 } else { 0 };

    println!("Strategy: {}", strategy_name);
    println!("Total Events: {}", iterations);
    println!("Buffer Size: {}", buffer_size);
    println!("Test Duration: {:.3} s ({} ms)", elapsed_duration.as_secs_f64(), elapsed_ms);
    println!("Throughput (events/sec): {}", ops_per_second);
    println!("Final Sum: {}, Expected Sum: {}", final_sum, expected_sum);

    if let Ok(batch_sizes) = batch_sizes_log.lock() {
        if !batch_sizes.is_empty() {
            let total_batches = batch_sizes.len();
            let avg_batch_size: f64 = batch_sizes.iter().sum::<u64>() as f64 / total_batches as f64;
            let min_batch_size = batch_sizes.iter().min().unwrap_or(&0);
            let max_batch_size = batch_sizes.iter().max().unwrap_or(&0);
            println!(
                "Batching: Avg Size: {:.2}, Min: {}, Max: {}, Total Batches: {}",
                avg_batch_size, min_batch_size, max_batch_size, total_batches
            );
        } else {
            println!("Batching: No batch size data collected.");
        }
    }

    assert_eq!(
        final_sum, expected_sum,
        "Sum mismatch for strategy {}! Final: {}, Expected: {}",
        strategy_name, final_sum, expected_sum
    );
    println!("--- Test for {} PASSED ---", strategy_name);
    Ok(())
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Rust Disruptor Consumer Batch Performance Tests Starting ===");

    const BUFFER_SIZE: usize = 1024 * 64; 
    const ITERATIONS: u64 = 1000 * 1000 * 50; // 50 million events

    run_spsc_batch_throughput_test::<BusySpinWaitStrategy>("BusySpinWaitStrategy (Batch)", BUFFER_SIZE, ITERATIONS)?;
    
    // Added BlockingWaitStrategy to the test runs
    run_spsc_batch_throughput_test::<BlockingWaitStrategy>("BlockingWaitStrategy (Batch)", BUFFER_SIZE, ITERATIONS)?;

    // add YieldingWaitStrategy
    run_spsc_batch_throughput_test::<YieldingWaitStrategy>("YieldingWaitStrategy (Batch)", BUFFER_SIZE, ITERATIONS)?;
    
    // Note: PhasedBackoffWaitStrategy requires specific timeouts and a non-default constructor,
    // so it would need a slightly different setup if you wanted to include it.
    // (e.g., PhasedBackoffWaitStrategy::new(Duration::from_micros(1), Duration::from_millis(1), BlockingWaitStrategy::default()))
    // and then pass that specific instance, or modify run_spsc_batch_throughput_test to accommodate non-Default strategies.

    println!("\n=== Rust Disruptor Consumer Batch Performance Tests Completed ===");
    Ok(())
}