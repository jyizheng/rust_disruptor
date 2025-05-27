// src/bin/basic_main.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::producer::Producer;
use rust_disruptor::sequencer::{ProducerMode};
use rust_disruptor::wait_strategy::BusySpinWaitStrategy;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// --- Producer Task Function (Copied from original main.rs) ---
fn producer_task(producer: Producer<MyEvent>, id: usize, fail_on_sequence: Option<u64>) {
    println!("[Producer {}] Producer thread started.", id);
    for i in 0..10 {
        let current_claimed_seq_before_next = producer.sequencer.producer_cursor.get();
        println!("[Producer {}] Attempting to claim sequence number {}", id, current_claimed_seq_before_next + 1);

        let claim_guard = producer.next();
        let sequence = claim_guard.sequence();

        unsafe {
            let event_entry = producer.get_mut(sequence);
            event_entry.value = (i + (id - 1) * 10) as u64;
        }

        if let Some(fail_seq) = fail_on_sequence {
            if sequence == fail_seq as i64 {
                println!("[Producer {}] Simulating failure: not publishing sequence number {}", id, sequence);
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        }

        claim_guard.publish();
        thread::sleep(Duration::from_millis(50));
    }
    println!("[Producer {}] Producer thread finished.", id);
}

// --- Consumer Task Function (Copied from original main.rs) ---
fn consumer_task(
    consumer: Consumer<MyEvent, BusySpinWaitStrategy>,
    processed_values_clone: Arc<Mutex<Vec<u64>>>,
    id: usize,
    total_events_to_consume: u64,
) {
    println!("[Consumer {}] Consumer thread started.", id);
    let mut count = 0;
    loop {
        let result = consumer.process_event(|event: &MyEvent| {
            println!("[Consumer {}] Processing event: {:?}", id, event);
            processed_values_clone.lock().unwrap().push(event.value);
        });

        if let Some(_) = result {
            count += 1;
            if count >= total_events_to_consume {
                println!("[Consumer {}] Processed all {} events. Exiting.", id, total_events_to_consume);
                break;
            }
        } else {
            thread::yield_now();
        }
    }
    println!("[Consumer {}] Consumer thread finished.", id);
}

fn main() {
    println!("\n--- Running basic multi-producer multi-consumer example (separate binary) ---");

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

    producer1_handle_basic.join().expect("Producer 1 thread panicked");
    producer2_handle_basic.join().expect("Producer 2 thread panicked");
    consumer1_handle_basic.join().expect("Consumer 1 thread panicked");
    consumer2_handle_basic.join().expect("Consumer 2 thread panicked");
    consumer3_handle_basic.join().expect("Consumer 3 thread panicked");

    let mut combined_final_values_basic: Vec<u64> = Vec::new();
    combined_final_values_basic.extend_from_slice(&processed_values_c1_basic.lock().unwrap());
    combined_final_values_basic.extend_from_slice(&processed_values_c2_basic.lock().unwrap());
    combined_final_values_basic.extend_from_slice(&processed_values_c3_basic.lock().unwrap());

    combined_final_values_basic.sort_unstable();

    println!("\nAll events processed. Final collected values (sorted, may contain duplicates from multiple consumers): {:?}", combined_final_values_basic);

    let mut expected_values_set_basic: HashSet<u64> = HashSet::new();
    for i in 0..NUM_EVENTS_PER_PRODUCER_BASIC {
        expected_values_set_basic.insert(i as u64);
        expected_values_set_basic.insert((i + NUM_EVENTS_PER_PRODUCER_BASIC) as u64);
    }

    let mut actual_unique_values_set_basic = HashSet::new();
    for &val in combined_final_values_basic.iter() {
        actual_unique_values_set_basic.insert(val);
    }

    assert_eq!(actual_unique_values_set_basic.len(), TOTAL_EVENTS_BASIC, "Number of unique events processed is incorrect.");
    assert_eq!(actual_unique_values_set_basic, expected_values_set_basic, "Set of processed values does not match expected set.");
    println!("Assertion passed: Processed values match expected values and are unique.");

    println!("Rust Disruptor basic multi-producer multi-consumer example finished.");
}
