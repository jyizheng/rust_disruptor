// src/bin/perf_affinity_main.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::sequencer::ProducerMode;
use rust_disruptor::wait_strategy::BusySpinWaitStrategy;
use rust_disruptor::wait_strategy::WaitStrategy; // Not directly used in main's scope

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Instant;

// Consumer Task Function (remains the same, takes core_id_option)
fn consumer_task_perf_test(
    consumer: Consumer<MyEvent, BusySpinWaitStrategy>,
    accumulated_sum_perf: Arc<AtomicU64>,
    iterations_to_process: u64,
    tx_perf: mpsc::Sender<()>,
    core_id_option: Option<core_affinity::CoreId>,
) {
    if let Some(core_id) = core_id_option {
        if !core_affinity::set_for_current(core_id) {
            eprintln!("[Perf Test Consumer] Warning: Failed to set CPU affinity for consumer thread on core {:?}", core_id);
        } else {
            println!("[Perf Test Consumer] Consumer thread affinity set to core {:?}", core_id);
        }
    }

    let mut processed_count_perf = 0;
    let mut local_sum: u64 = 0;

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
                unsafe {
                    let event_perf = consumer.ring_buffer.get(seq_to_process);
                    local_sum += event_perf.value;
                }
                processed_count_perf += 1;
            }
            consumer.sequence.set(highest_available_by_wait_strategy);
        }
    }
    accumulated_sum_perf.fetch_add(local_sum, Ordering::Relaxed);

    if tx_perf.send(()).is_err() {
        eprintln!("[Performance Test Consumer] Could not send completion signal. Receiver may have been dropped.");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> { // Added Result for error propagation
    println!("\n--- Running One-to-One Ordered Throughput Test (Separate Binary, Using core_affinity) ---");

    const BUFFER_SIZE_PERF: usize = 65536;
    const ITERATIONS_PERF: u64 = 100_000_000;
    let expected_sum_perf: u64 = if ITERATIONS_PERF > 0 { (ITERATIONS_PERF * (ITERATIONS_PERF - 1)) / 2 } else { 0 };

    println!("[Main] Initializing Disruptor...");
    let mut disruptor_perf = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        BUFFER_SIZE_PERF,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );

    // `producer_perf` will be used by the main thread.
    let producer_perf = disruptor_perf.create_producer();
    let consumer_perf_for_thread = disruptor_perf.create_consumer(vec![]); // Renamed for clarity

    let accumulated_sum_perf = Arc::new(AtomicU64::new(0));
    let (tx_consumer_done, rx_consumer_done) = mpsc::channel(); // Renamed for clarity

    let consumer_thread_sum_clone_perf = Arc::clone(&accumulated_sum_perf);
    let consumer_thread_tx_clone_perf = tx_consumer_done.clone();

    // Get core IDs
    let core_ids = core_affinity::get_core_ids();
    let (producer_core_id_opt, consumer_core_id_opt) = if let Some(ids) = &core_ids {
        if ids.len() >= 2 {
            println!("[Main] Available cores: {:?}", ids);
            (Some(ids[0]), Some(ids[1])) // Assign first to producer (main), second to consumer
        } else if ids.len() == 1 {
            println!("[Main] Warning: Only 1 core available. Both producer and consumer will use core {:?}", ids[0]);
            (Some(ids[0]), Some(ids[0]))
        } else {
            println!("[Main] Warning: No core IDs retrieved. Running without affinity.");
            (None, None)
        }
    } else {
        println!("[Main] Warning: Could not get core IDs. Running without affinity.");
        (None, None)
    };

    println!("[Main] Spawning consumer thread...");
    let consumer_handle_perf = thread::Builder::new()
        .name("perf-consumer-thread".to_string())
        .spawn(move || {
            consumer_task_perf_test(
                consumer_perf_for_thread, // Moved into thread
                consumer_thread_sum_clone_perf,
                ITERATIONS_PERF,
                consumer_thread_tx_clone_perf,
                consumer_core_id_opt,
            );
        }).expect("Failed to spawn consumer thread");

    // Set affinity for the main thread (which will act as the producer)
    if let Some(core_id) = producer_core_id_opt {
        if !core_affinity::set_for_current(core_id) {
            eprintln!("[Main/Producer] Warning: Failed to set CPU affinity for main/producer thread on core {:?}", core_id);
        } else {
            println!("[Main/Producer] Main/Producer thread affinity set to core {:?}", core_id);
        }
    }
    
    // Small pause to allow consumer thread to set its affinity and start
    thread::sleep(std::time::Duration::from_millis(100));

    println!("[Main/Producer] Producer loop starting (in main).");
    let start_time_perf = Instant::now(); // Start timing before producer loop

    for i in 0..ITERATIONS_PERF {
        let claim_guard_perf = producer_perf.next(); // `producer_perf` is owned by main
        let sequence_perf = claim_guard_perf.sequence();
        unsafe {
            let event_perf = producer_perf.get_mut(sequence_perf);
            event_perf.value = i;
        }
        claim_guard_perf.publish();
    }
    println!("[Main/Producer] Producer loop completed (in main).");

    println!("[Main] Waiting for consumer to process all events...");
    match rx_consumer_done.recv_timeout(std::time::Duration::from_secs(180)) { // Increased timeout
        Ok(_) => println!("[Main] Consumer signaled completion."),
        Err(e) => {
            eprintln!("[Main] Error or timeout waiting for consumer completion: {:?}. Current sum: {}", e, accumulated_sum_perf.load(Ordering::Relaxed));
            // Propagate the error to stop the program if the test fails due to timeout
            return Err(Box::new(e));
        }
    }
    let end_time_perf = Instant::now(); // End timing after consumer is done

    println!("[Main] Waiting for consumer thread to join...");
    // This is the ONLY join call for consumer_handle_perf
    consumer_handle_perf.join().expect("Consumer thread panicked");
    println!("[Main] Consumer thread joined.");
    // Log messages from consumer task itself are better for its completion details

    let elapsed_ms = end_time_perf.duration_since(start_time_perf).as_millis() as u64;

    println!("\n--- Performance Test Results ---"); // Changed
    println!("Total Iterations (ITERATIONS): {}", ITERATIONS_PERF);
    println!("Buffer Size (BUFFER_SIZE): {}", BUFFER_SIZE_PERF);
    println!("Elapsed Time: {} ms", elapsed_ms); // Changed

    if elapsed_ms > 0 {
        let ops_per_second = (ITERATIONS_PERF * 1000) / elapsed_ms;
        println!("Throughput: {} ops/sec", ops_per_second); // Changed
    } else {
        println!("Throughput: N/A (elapsed time is 0 or too short)"); // Changed
    }

    let final_sum = accumulated_sum_perf.load(Ordering::SeqCst);
    println!("Final Sum: {}", final_sum); // Changed
    println!("Expected Sum: {}", expected_sum_perf); // Changed

    assert_eq!(final_sum, expected_sum_perf, "Sum mismatch!"); // Changed
    println!("Assertion passed: Sum is correct."); // Changed
    Ok(())
}
