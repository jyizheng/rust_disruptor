// src/bin/wait_strategy_perf_main.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::Event as MyEventTrait; // Alias to avoid conflict
use rust_disruptor::producer::Producer;
use rust_disruptor::sequencer::ProducerMode;
use rust_disruptor::wait_strategy::{
    BusySpinWaitStrategy, BlockingWaitStrategy, PhasedBackoffWaitStrategy, WaitStrategy,
    YieldingWaitStrategy,
};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc}; // Barrier not needed for SPSC if producer starts first
use std::thread;
use std::time::{Duration, Instant};

// --- Event Definition ---
#[derive(Debug, Default, Clone, Copy)]
struct MyValueEvent {
    value: u64, // Using u64 to match typical perf test patterns
}
impl MyEventTrait for MyValueEvent {}

// --- Publisher Task ---
fn value_publisher_task(
    producer: Producer<MyValueEvent>,
    iterations: u64,
) {
    // println!("[Publisher-{}] Starting...", publisher_id);
    for i in 0..iterations {
        let guard = producer.next();
        let sequence = guard.sequence();
        unsafe {
            let event = producer.get_mut(sequence);
            event.value = i;
        }
        guard.publish();
    }
    // println!("[Publisher-{}] Finished.", publisher_id);
}

// --- Consumer Task ---
fn spsc_consumer_task<W: WaitStrategy + Send + Sync + 'static>(
    consumer: Consumer<MyValueEvent, W>, // W is the concrete WaitStrategy type
    accumulated_sum: Arc<AtomicU64>,
    iterations_to_process: u64,
    completion_tx: mpsc::Sender<()>,
) {
    // println!("[Consumer] Starting...");
    let mut processed_count = 0;
    let mut current_sum: u64 = 0; // Local sum to reduce atomic contention

    while processed_count < iterations_to_process {
        let next_sequence_to_try = consumer.sequence.get() + 1;

        let highest_available = consumer.wait_strategy.wait_for(
            next_sequence_to_try,
            Arc::clone(&consumer.sequencer),
            &consumer.gating_sequences_for_wait, // For SPSC, this contains producer_cursor
            Arc::clone(&consumer.sequence),
        );

        if highest_available >= next_sequence_to_try {
            for seq_to_process in next_sequence_to_try..=highest_available {
                if processed_count >= iterations_to_process {
                    break;
                }
                // Add unsafe block because RingBuffer::get() is unsafe
                unsafe {
                    let event = consumer.ring_buffer.get(seq_to_process);
                    current_sum += event.value; // Add to local sum
                }
                processed_count += 1;
            }
            consumer.sequence.set(highest_available);
        }
        // No yield here, rely on wait_strategy
    }
    accumulated_sum.fetch_add(current_sum, Ordering::Relaxed); // Add local sum to atomic sum once
    completion_tx.send(()).expect("Consumer: Could not send completion signal");
    // println!("[Consumer] Finished. Processed {} events.", processed_count);
}

// --- Generic Test Runner ---
fn run_spsc_throughput_test<W: WaitStrategy + Clone + Send + Sync + Default + 'static>(
    strategy_name: &str,
    buffer_size: usize,
    iterations: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "\n--- Running SPSC Throughput Test for: {} ---",
        strategy_name
    );

    let wait_strategy_instance = W::default(); // Create instance of the wait strategy

    let mut disruptor = Disruptor::<MyValueEvent, W>::new(
        buffer_size,
        wait_strategy_instance, // Pass the concrete wait strategy instance
        ProducerMode::Single,
    );

    let producer = disruptor.create_producer();
    // For SPSC, the consumer depends only on the producer (implicitly handled by Sequencer)
    let consumer = disruptor.create_consumer(vec![]);

    let accumulated_sum = Arc::new(AtomicU64::new(0));
    let (completion_tx, completion_rx) = mpsc::channel();

    let consumer_sum_clone = Arc::clone(&accumulated_sum);
    let consumer_thread = thread::Builder::new()
        .name(format!("spsc-consumer-{}", strategy_name))
        .spawn(move || {
            spsc_consumer_task(consumer, consumer_sum_clone, iterations, completion_tx);
        })?;

    // Give consumer a moment to fully initialize and register its sequence
    // This helps ensure the producer doesn't start publishing before the consumer
    // is ready and its sequence is gating the producer.
    // In a real robust setup, a barrier or a check on sequencer's gating sequences might be used.
    thread::sleep(Duration::from_millis(50));


    let producer_thread = thread::Builder::new()
        .name(format!("spsc-producer-{}", strategy_name))
        .spawn(move || {
            value_publisher_task(producer, iterations);
        })?;

    let start_time = Instant::now();

    producer_thread.join().expect("Producer thread panicked");
    // println!("[Test-{}] Producer finished.", strategy_name);

    // Wait for the consumer to signal that it has processed all events
    completion_rx.recv_timeout(Duration::from_secs(120))?; // Timeout after 2 minutes
    let end_time = Instant::now();
    // println!("[Test-{}] Consumer signaled completion.", strategy_name);


    consumer_thread.join().expect("Consumer thread panicked");
    // println!("[Test-{}] Consumer thread joined.", strategy_name);


    let elapsed_duration = end_time.duration_since(start_time);
    let elapsed_ms = elapsed_duration.as_millis();
    let ops_per_second = if elapsed_ms > 0 {
        (iterations * 1000) / elapsed_ms as u64
    } else {
        0 // Avoid division by zero
    };

    let final_sum = accumulated_sum.load(Ordering::SeqCst);
    let expected_sum = if iterations > 0 { (iterations * (iterations - 1)) / 2 } else { 0 };


    println!("Strategy: {}", strategy_name);
    println!("Total Events: {}", iterations);
    println!("Buffer Size: {}", buffer_size);
    println!("Test Duration: {:.3} s ({} ms)", elapsed_duration.as_secs_f64(), elapsed_ms);
    println!("Throughput (events/sec): {}", ops_per_second);
    println!("Final Sum: {}, Expected Sum: {}", final_sum, expected_sum);

    assert_eq!(
        final_sum, expected_sum,
        "Sum mismatch for strategy {}!",
        strategy_name
    );
    println!("--- Test for {} PASSED ---", strategy_name);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Rust Disruptor Wait Strategy Performance Tests Starting ===");

    const BUFFER_SIZE: usize = 1024 * 64; // 65536
    const ITERATIONS: u64 = 1000 * 1000 * 50; // 50 million events

    run_spsc_throughput_test::<BusySpinWaitStrategy>("BusySpinWaitStrategy", BUFFER_SIZE, ITERATIONS)?;
    run_spsc_throughput_test::<YieldingWaitStrategy>("YieldingWaitStrategy", BUFFER_SIZE, ITERATIONS)?;
    run_spsc_throughput_test::<BlockingWaitStrategy>("BlockingWaitStrategy", BUFFER_SIZE, ITERATIONS)?;

    // PhasedBackoffWaitStrategy needs specific timeouts.
    // If its `default()` provides sensible values, it can be used like the others.
    // Otherwise, you might need a specialized call or a way to pass config.
    // Assuming PhasedBackoffWaitStrategy::default() is implemented and sensible:
    if cfg!(feature = "phased_backoff_test") { // Optional: use a feature flag if it's slow or complex
        println!("Note: PhasedBackoffWaitStrategy test might take longer or require specific timeout tuning.");
         run_spsc_throughput_test::<PhasedBackoffWaitStrategy>("PhasedBackoffWaitStrategy", BUFFER_SIZE, ITERATIONS)?;
    } else {
        println!("\nSkipping PhasedBackoffWaitStrategy test. Enable with '--features phased_backoff_test'");
    }


    println!("\n=== Rust Disruptor Wait Strategy Performance Tests Completed ===");
    Ok(())
}

