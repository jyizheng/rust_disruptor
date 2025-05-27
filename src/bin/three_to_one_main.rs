// src/bin/three_to_one_main.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::Event as MyEventTrait;
use rust_disruptor::producer::Producer;
use rust_disruptor::sequencer::ProducerMode;
use rust_disruptor::wait_strategy::{BusySpinWaitStrategy, WaitStrategy};

use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread::{self};
use std::time::Instant;

// --- Event Definition (MyValueEvent) ---
#[derive(Debug, Default, Clone, Copy)]
struct MyValueEvent {
    value: i64,
}
impl MyEventTrait for MyValueEvent {}

// --- ValueAdditionHandler ---
struct ValueAdditionHandler {
    value_sum: i64,
    latch_sender: std::sync::mpsc::SyncSender<()>,
    expected_event_count: u64,
    event_count: u64,
    batch_count: u64,
    id: usize,
}

impl ValueAdditionHandler {
    fn new(latch_sender: std::sync::mpsc::SyncSender<()>, expected_count: u64, id: usize) -> Self {
        Self {
            value_sum: 0,
            latch_sender,
            expected_event_count: expected_count,
            event_count: 0,
            batch_count: 0,
            id,
        }
    }
    fn on_event(&mut self, event: &MyValueEvent, _sequence: i64, end_of_batch: bool) {
        self.value_sum += event.value;
        self.event_count += 1;

        if end_of_batch {
            self.batch_count += 1;
        }

        if self.event_count >= self.expected_event_count {
            if self.latch_sender.try_send(()).is_ok() {
                println!("[Handler-{}] Sent completion signal. Processed {} events.", self.id, self.event_count);
            }
        }
    }
    fn get_sum(&self) -> i64 { self.value_sum }
    fn get_event_count(&self) -> u64 { self.event_count }
    fn get_batch_count(&self) -> u64 { self.batch_count }
}

// --- Publisher Task ---
fn value_publisher_task(
    producer: Producer<MyValueEvent>,
    barrier: Arc<Barrier>,
    iterations_per_publisher: u64,
    publisher_id: usize,
) {
    barrier.wait();
    // let progress_interval = (iterations_per_publisher / 10).max(1);
    for i in 0..iterations_per_publisher {
        let guard = producer.next();
        let sequence = guard.sequence();
        unsafe {
            let event = producer.get_mut(sequence);
            event.value = i as i64;
        }
        guard.publish();
    }
    println!("[Publisher-{}] Finished publishing {} events.", publisher_id, iterations_per_publisher);
}

// --- Consumer Task ---
fn consumer_task_for_handler(
    consumer: Consumer<MyValueEvent, BusySpinWaitStrategy>,
    handler_mutex: Arc<Mutex<ValueAdditionHandler>>,
    barrier: Arc<Barrier>,
) {
    barrier.wait();
    let handler_id_for_log;
    let expected_total_events_for_handler;
    {
        let h_guard = handler_mutex.lock().unwrap();
        handler_id_for_log = h_guard.id;
        expected_total_events_for_handler = h_guard.expected_event_count;
    }

    loop {
        let next_sequence_to_try = consumer.sequence.get() + 1;
        let highest_available = consumer.wait_strategy.wait_for(
            next_sequence_to_try,
            Arc::clone(&consumer.sequencer),
            &consumer.gating_sequences_for_wait,
            Arc::clone(&consumer.sequence),
        );

        if highest_available >= next_sequence_to_try {
            let mut handler = handler_mutex.lock().unwrap();
            if handler.get_event_count() >= handler.expected_event_count {
                break;
            }
            for s in next_sequence_to_try..=highest_available {
                let end_of_batch = s == highest_available;
                unsafe {
                    let event = consumer.ring_buffer.get(s);
                    handler.on_event(event, s, end_of_batch);
                }
                if handler.get_event_count() >= handler.expected_event_count {
                    break;
                }
            }
            let current_handler_event_count = handler.get_event_count();
            drop(handler);
            consumer.sequence.set(highest_available);
            if current_handler_event_count >= expected_total_events_for_handler {
                break;
            }
        } else {
            let handler = handler_mutex.lock().unwrap();
            let still_expecting_events = handler.get_event_count() < handler.expected_event_count;
            drop(handler);
            if !still_expecting_events {
                break;
            }
        }
    }
    println!("[Consumer -> Handler-{}] Finished processing loop.", handler_id_for_log);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Three-To-One Sequenced Throughput Test (Rust)...");

    const NUM_PUBLISHERS: usize = 3;
    const BUFFER_SIZE: usize = 1024 * 64;
    const ORIGINAL_TARGET_TOTAL_ITERATIONS: u64 = 1000 * 1000 * 20; // 20 million

    // Calculate what each publisher will actually do due to integer division
    let iterations_per_publisher = ORIGINAL_TARGET_TOTAL_ITERATIONS / NUM_PUBLISHERS as u64;

    // Calculate the actual total number of events that will be produced and expected by the handler
    let actual_total_events_to_process = iterations_per_publisher * NUM_PUBLISHERS as u64;

    println!("[Main] Config: {} Publishers, Buffer Size: {}, Original Target Events: {}, Events/Pub: {}, Actual Total Events to Process: {}",
             NUM_PUBLISHERS, BUFFER_SIZE, ORIGINAL_TARGET_TOTAL_ITERATIONS, iterations_per_publisher, actual_total_events_to_process);

    println!("[Main] Initializing Disruptor...");
    let mut disruptor = Disruptor::<MyValueEvent, BusySpinWaitStrategy>::new(
        BUFFER_SIZE,
        BusySpinWaitStrategy::default(),
        ProducerMode::Multi,
    );

    println!("[Main] Creating {} producers...", NUM_PUBLISHERS);
    let mut producers = Vec::with_capacity(NUM_PUBLISHERS);
    for _i in 0..NUM_PUBLISHERS {
        producers.push(disruptor.create_producer());
    }

    println!("[Main] Setting up handler and completion latch (expecting {} events)...", actual_total_events_to_process);
    let (latch_tx, latch_rx) = mpsc::sync_channel(1);
    // IMPORTANT: Handler expects the *actual* number of events that will be produced
    let handler_shared = Arc::new(Mutex::new(ValueAdditionHandler::new(latch_tx, actual_total_events_to_process, 0)));

    println!("[Main] Creating consumer...");
    let consumer = disruptor.create_consumer(vec![]);

    println!("[Main] Setting up barrier for {} publishers + 1 consumer.", NUM_PUBLISHERS);
    let barrier = Arc::new(Barrier::new(NUM_PUBLISHERS + 1));
    let mut publisher_handles = Vec::with_capacity(NUM_PUBLISHERS);

    println!("[Main] Spawning publisher threads...");
    for i in 0..NUM_PUBLISHERS {
        let prod = producers.remove(0);
        let barrier_clone = Arc::clone(&barrier);
        let handle = thread::Builder::new().name(format!("publisher-{}", i)).spawn(move || {
            value_publisher_task(prod, barrier_clone, iterations_per_publisher, i);
        })?;
        publisher_handles.push(handle);
    }

    println!("[Main] Spawning consumer thread...");
    let consumer_barrier_clone = Arc::clone(&barrier);
    let consumer_handler_clone = Arc::clone(&handler_shared);
    let consumer_handle = thread::Builder::new().name("consumer-thread".to_string()).spawn(move || {
        consumer_task_for_handler(consumer, consumer_handler_clone, consumer_barrier_clone);
    })?;

    println!("[Main] All threads spawned. Waiting for test to run...");
    let start_time = Instant::now();

    println!("[Main] Waiting for publishers to complete their iterations...");
    for (i, handle) in publisher_handles.into_iter().enumerate() {
        handle.join().expect(&format!("Publisher {} thread panicked", i));
    }
    println!("[Main] All publishers have completed their iterations.");

    println!("[Main] Waiting for handler to process all {} events...", actual_total_events_to_process);
    match latch_rx.recv_timeout(std::time::Duration::from_secs(60)) {
        Ok(_) => println!("[Main] Handler signaled completion."),
        Err(e) => {
            eprintln!("[Main] Error or timeout waiting for handler completion: {:?}. Current handler state:", e);
            if let Ok(handler_guard) = handler_shared.lock() {
                 eprintln!("[Main] Handler processed {}/{} events.", handler_guard.get_event_count(), handler_guard.expected_event_count);
            }
            return Err(format!("Timeout or error waiting for handler. Expected {} events. Error: {:?}", actual_total_events_to_process, e).into());
        }
    }
    let end_time = Instant::now();

    println!("[Main] Joining consumer thread...");
    consumer_handle.join().expect("Consumer thread panicked");
    println!("[Main] Consumer thread joined.");

    let elapsed_duration = end_time.duration_since(start_time);
    let elapsed_ms = elapsed_duration.as_millis();

    // Throughput calculation should use actual_total_events_to_process
    let ops_per_second = if elapsed_ms > 0 {
        (actual_total_events_to_process * 1000) / elapsed_ms as u64
    } else {
        0
    };

    let final_handler_state = handler_shared.lock().unwrap();
    let final_sum = final_handler_state.get_sum();
    let final_event_count = final_handler_state.get_event_count();
    let final_batch_count = final_handler_state.get_batch_count();
    drop(final_handler_state);

    let mut expected_sum: i64 = 0;
    for _ in 0..NUM_PUBLISHERS {
        let n = iterations_per_publisher;
        if n > 0 {
            let sum_per_publisher_u64: u64 = (n * (n - 1)) / 2;
            expected_sum += sum_per_publisher_u64 as i64;
        }
    }

    println!("\n--- Three-To-One Sequenced Test Results (Rust) ---");
    println!("Target Total Iterations (Events): {}", ORIGINAL_TARGET_TOTAL_ITERATIONS);
    println!("Actual Events Produced & Expected by Handler: {}", actual_total_events_to_process);
    println!("Iterations Per Publisher: {}", iterations_per_publisher);
    println!("Buffer Size: {}", BUFFER_SIZE);
    println!("Test Duration: {:.3} s ({} ms)", elapsed_duration.as_secs_f64(), elapsed_ms);
    println!("Throughput (events/sec): {}", ops_per_second);
    println!("Handler - Events Processed: {}", final_event_count);
    println!("Handler - Batches Processed: {}", final_batch_count);
    println!("Handler - Sum of Values: {}", final_sum);
    println!("Expected Sum of Values: {}", expected_sum);

    if final_sum != expected_sum {
        eprintln!("Error: Sum mismatch! Expected {}, got {}", expected_sum, final_sum);
        // return Err("Sum mismatch".into()); // Allow event count check to also run
    }
    // Check if the handler processed at least the actual total. It might process slightly more if the
    // completion signal is slightly delayed and it picks up a few more from a nearly full batch.
    // The primary check is that it *at least* processed what was expected.
    if final_event_count < actual_total_events_to_process {
         eprintln!("Error: Event count too low! Expected at least {}, got {}", actual_total_events_to_process, final_event_count);
        return Err(format!("Event count too low. Expected {}, got {}.", actual_total_events_to_process, final_event_count).into());
    }
     if final_sum != expected_sum { // Re-check sum if event count passed, to ensure this error is also caught
        return Err("Sum mismatch".into());
    }

    println!("Test completed successfully.");
    Ok(())
}
