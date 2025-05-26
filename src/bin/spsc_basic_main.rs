// src/bin/spsc_basic_main.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::producer::Producer;
use rust_disruptor::sequencer::ProducerMode;
use rust_disruptor::wait_strategy::BusySpinWaitStrategy;

use std::sync::{Arc, Mutex};
use std::thread;

// --- Producer Task Function ---
fn producer_task_basic(producer: Producer<MyEvent>, id: usize, num_events: u64, start_value: u64) {
    println!("[SPSC_Producer {}] Starting. Will produce {} events starting from value {}.", id, num_events, start_value);
    for i in 0..num_events {
        let claim_guard = producer.next();
        let sequence = claim_guard.sequence();
        unsafe {
            let event_entry = producer.get_mut(sequence);
            event_entry.value = start_value + i;
        }
        claim_guard.publish();
    }
    println!("[SPSC_Producer {}] Finished producing {} events.", id, num_events);
}

// --- Consumer Task Function ---
fn consumer_task_basic(
    consumer: Consumer<MyEvent, BusySpinWaitStrategy>,
    processed_values_clone: Arc<Mutex<Vec<u64>>>,
    id: usize,
    total_events_to_consume: u64,
) {
    println!("[SPSC_Consumer {}] Starting. Expecting {} events.", id, total_events_to_consume);
    let mut count = 0;
    let mut received_values_local = Vec::new();

    loop {
        // This closure needs to be FnMut, requiring Consumer::process_event to accept it.
        let result = consumer.process_event(|event: &MyEvent| {
            received_values_local.push(event.value);
        });

        if let Some(_sequence_processed) = result {
            count += 1;
            if count >= total_events_to_consume {
                // println!("[SPSC_Consumer {}] Processed expected {} events. Exiting.", id, total_events_to_consume);
                break;
            }
        } else {
            // In an SPSC scenario with a fixed number of events, if process_event returns None
            // before all events are consumed, it implies the producer is slower or done,
            // and the WaitStrategy determined no event was immediately available.
            // The loop should continue until `count` reaches `total_events_to_consume`.
            // A yield can prevent a tight spin if the producer is slow.
            if count < total_events_to_consume { // Only yield if we are still expecting events
                thread::yield_now();
            } else {
                // This case should ideally not be hit if logic is correct and producer produces all events.
                // println!("[SPSC_Consumer {}] No event, but all expected events processed. Exiting.", id);
                break;
            }
        }
    }
    let mut guard = processed_values_clone.lock().unwrap();
    guard.extend(received_values_local);
    println!("[SPSC_Consumer {}] Finished. Processed {} events.", id, count);
}

fn single_producer_single_consumer_basic_test() {
    println!("\n--- Running: Single-Producer Single-Consumer Basic Test (Dedicated Binary) ---");

    const BUFFER_SIZE_SPSC: usize = 16; // Can be small for basic correctness test
    const NUM_EVENTS_SPSC: u64 = 2000; // A moderate number of events for a quick test

    println!("[SPSC_Test] Buffer Size: {}, Num Events: {}", BUFFER_SIZE_SPSC, NUM_EVENTS_SPSC);

    println!("[SPSC_Test] Initializing Disruptor with ProducerMode::Single...");
    let mut disruptor_spsc = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        BUFFER_SIZE_SPSC,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single, // Crucial for SPSC
    );

    println!("[SPSC_Test] Creating producer...");
    let producer_spsc = disruptor_spsc.create_producer();

    println!("[SPSC_Test] Creating consumer...");
    let consumer_spsc = disruptor_spsc.create_consumer(vec![]); // Single consumer, no dependencies

    let processed_values_spsc = Arc::new(Mutex::new(Vec::<u64>::new()));
    let processed_values_clone_spsc = Arc::clone(&processed_values_spsc);

    println!("[SPSC_Test] Spawning consumer thread...");
    let consumer_handle_spsc = thread::Builder::new()
        .name("spsc-consumer-thread".to_string())
        .spawn(move || {
            consumer_task_basic(consumer_spsc, processed_values_clone_spsc, 1, NUM_EVENTS_SPSC);
        })
        .expect("Failed to spawn SPSC consumer thread");

    println!("[SPSC_Test] Spawning producer thread...");
    let producer_handle_spsc = thread::Builder::new()
        .name("spsc-producer-thread".to_string())
        .spawn(move || {
            producer_task_basic(producer_spsc, 1, NUM_EVENTS_SPSC, 1000); // Start values from 1000
        })
        .expect("Failed to spawn SPSC producer thread");

    producer_handle_spsc.join().expect("SPSC Producer panicked");
    println!("[SPSC_Test] Producer finished and joined.");
    consumer_handle_spsc.join().expect("SPSC Consumer panicked");
    println!("[SPSC_Test] Consumer finished and joined.");

    let final_values_spsc = processed_values_spsc.lock().unwrap();
    println!("[SPSC_Test] Consumer processed values (count: {}): {:?}", final_values_spsc.len(), *final_values_spsc);

    assert_eq!(final_values_spsc.len(), NUM_EVENTS_SPSC as usize, "SPSC: Incorrect number of events processed.");

    let mut expected_values_spsc = Vec::new();
    for i in 0..NUM_EVENTS_SPSC {
        expected_values_spsc.push(1000 + i);
    }
    assert_eq!(*final_values_spsc, expected_values_spsc, "SPSC: Processed values do not match expected values.");

    println!("[SPSC_Test] Assertions passed.");
    println!("--- SPSC Basic Test Completed Successfully ---");
}

fn main() {
    println!("=== Rust Disruptor SPSC Basic Test Starting ===");
    single_producer_single_consumer_basic_test();
    println!("\n=== Rust Disruptor SPSC Basic Test Completed ===");
}


