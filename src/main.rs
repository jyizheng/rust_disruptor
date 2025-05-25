// src/main.rs

mod event;
mod sequencer;
mod ring_buffer;
mod wait_strategy;
mod producer;
mod consumer;
mod disruptor; // New module!

use event::MyEvent;
use wait_strategy::BusySpinWaitStrategy;
use disruptor::Disruptor;
use std::thread;
use std::time::Duration;
use std::sync::{Arc, Mutex};

fn main() {
    println!("Starting Rust Disruptor example...");

    const BUFFER_SIZE: usize = 16; // Must be a power of two
    let mut disruptor = Disruptor::new(BUFFER_SIZE, BusySpinWaitStrategy);

    // Create a producer
    let mut producer = disruptor.create_producer();

    // Create a consumer
    let consumer = disruptor.create_consumer(None); // No dependent sequence for simplicity

    // Use Arc<Mutex<Vec<usize>>> to collect processed values in a thread-safe way
    let processed_values = Arc::new(Mutex::new(Vec::new()));
    let processed_values_consumer_clone = Arc::clone(&processed_values);

    // Start consumer thread
    let consumer_handle = thread::spawn(move || {
        println!("[Consumer] Consumer thread started.");
        let mut count = 0;
        let mut expected_value = 0;

        loop {
            // Process events one by one
            let result = consumer.process_event(|event: &MyEvent| {
                println!("[Consumer] Processed event: {:?}", event);
                processed_values_consumer_clone.lock().unwrap().push(event.value);
            });

            if let Some(_) = result {
                count += 1;
                // In a real scenario, you'd have a way to stop the consumer,
                // e.g., a graceful shutdown signal or a specific "end" event.
                // For this example, we'll process a fixed number of events and break.
                if count >= 10 { // Let's process 10 events
                    println!("[Consumer] Processed 10 events. Exiting.");
                    break;
                }
            } else {
                // With BusySpinWaitStrategy, this branch is rarely taken
                // as it spins until an event is available.
                // A more advanced strategy might return None if no event
                // and then the thread could yield or sleep.
                thread::yield_now(); // Yield to avoid 100% CPU if nothing was there (though BusySpin typically won't yield here)
            }
        }
        println!("[Consumer] Consumer thread finished.");
    });

    // Start producer thread
    let producer_handle = thread::spawn(move || {
        println!("[Producer] Producer thread started.");
        for i in 0..10 { // Produce 10 events
            let sequence = producer.next(); // Claim the next slot
            let event = producer.get_mut(sequence); // Get mutable reference to the event slot
            event.value = i; // Set the event data
            println!("[Producer] Publishing event: value={}, sequence={}", i, sequence);
            producer.publish(sequence); // Publish the event
            thread::sleep(Duration::from_millis(100)); // Simulate some work
        }
        println!("[Producer] Producer thread finished.");
    });


    // Wait for producer and consumer threads to finish
    producer_handle.join().expect("Producer thread panicked");
    consumer_handle.join().expect("Consumer thread panicked");

    let final_values = processed_values.lock().unwrap();
    println!("\nAll events processed. Final collected values: {:?}", *final_values);

    // Verify the output
    let expected_values: Vec<usize> = (0..10).collect();
    assert_eq!(*final_values, expected_values);
    println!("Assertion passed: Processed values match expected values.");

    println!("Rust Disruptor example finished.");
}