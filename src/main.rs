// src/main.rs (MODIFIED for Multi-Producer Example)

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
use std::collections::HashSet; // To check for unique values

fn main() {
    println!("Starting Rust Disruptor multi-producer example...");

    const BUFFER_SIZE: usize = 16;
    let mut disruptor = Disruptor::new(BUFFER_SIZE, BusySpinWaitStrategy);

    // Create multiple producers
    let producer1 = disruptor.create_producer();
    let producer2 = disruptor.create_producer();

    // Create a consumer
    let consumer = disruptor.create_consumer(None);

    // Use Arc<Mutex<Vec<usize>>> to collect processed values in a thread-safe way
    let processed_values = Arc::new(Mutex::new(Vec::new()));
    let processed_values_consumer_clone = Arc::clone(&processed_values);

    // Determine total events to produce
    const NUM_EVENTS_PER_PRODUCER: usize = 10;
    const TOTAL_EVENTS: usize = NUM_EVENTS_PER_PRODUCER * 2; // Two producers

    // Start consumer thread
    let consumer_handle = thread::spawn(move || {
        println!("[Consumer] Consumer thread started.");
        let mut count = 0;

        loop {
            let result = consumer.process_event(|event: &MyEvent| {
                // println!("[Consumer] Processed event: {:?}", event); // Too verbose for many events
                processed_values_consumer_clone.lock().unwrap().push(event.value);
            });

            if let Some(_) = result {
                count += 1;
                if count >= TOTAL_EVENTS { // Process all expected events
                    println!("[Consumer] Processed all {} events. Exiting.", TOTAL_EVENTS);
                    break;
                }
            } else {
                thread::yield_now();
            }
        }
        println!("[Consumer] Consumer thread finished.");
    });

    // Start producer 1 thread
    let producer1_handle = thread::spawn(move || {
        println!("[Producer 1] Producer thread started.");
        for i in 0..NUM_EVENTS_PER_PRODUCER {
            let sequence = producer1.next();
            let event = producer1.get_mut(sequence);
            event.value = i; // Assign value based on its own producer index
            // println!("[Producer 1] Publishing event: value={}, sequence={}", i, sequence); // Too verbose
            producer1.publish(sequence);
            thread::sleep(Duration::from_millis(50)); // Shorter sleep to see more interleaving
        }
        println!("[Producer 1] Producer thread finished.");
    });

    // Start producer 2 thread
    let producer2_handle = thread::spawn(move || {
        println!("[Producer 2] Producer thread started.");
        for i in 0..NUM_EVENTS_PER_PRODUCER {
            let sequence = producer2.next();
            let event = producer2.get_mut(sequence);
            event.value = i + NUM_EVENTS_PER_PRODUCER; // Assign unique values to distinguish producers
            // println!("[Producer 2] Publishing event: value={}, sequence={}", event.value, sequence); // Too verbose
            producer2.publish(sequence);
            thread::sleep(Duration::from_millis(70)); // Different sleep to observe interleaving
        }
        println!("[Producer 2] Producer thread finished.");
    });


    // Wait for all threads to finish
    producer1_handle.join().expect("Producer 1 thread panicked");
    producer2_handle.join().expect("Producer 2 thread panicked");
    consumer_handle.join().expect("Consumer thread panicked");

    let final_values_locked = processed_values.lock().unwrap();
    let mut final_values: Vec<usize> = final_values_locked.clone();
    final_values.sort_unstable(); // Sort to make comparison easier, order isn't guaranteed

    println!("\nAll events processed. Final collected values (sorted): {:?}", final_values);

    // Verify the output: check for uniqueness and correct number of events
    let mut expected_values_set = HashSet::new();
    for i in 0..NUM_EVENTS_PER_PRODUCER {
        expected_values_set.insert(i); // From producer 1
        expected_values_set.insert(i + NUM_EVENTS_PER_PRODUCER); // From producer 2
    }

    let mut actual_values_set = HashSet::new();
    for &val in final_values.iter() {
        actual_values_set.insert(val);
    }

    assert_eq!(actual_values_set.len(), TOTAL_EVENTS, "Incorrect number of unique events processed.");
    assert_eq!(actual_values_set, expected_values_set, "Processed values do not match expected set of values.");
    println!("Assertion passed: Processed values match expected values and are unique.");

    println!("Rust Disruptor multi-producer example finished.");
}