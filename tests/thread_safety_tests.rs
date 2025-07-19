// tests/thread_safety_tests.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::sequencer::ProducerMode;
use rust_disruptor::wait_strategy::{BusySpinWaitStrategy, BlockingWaitStrategy};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn test_single_producer_multiple_consumers() {
    let capacity = 1024;
    let num_consumers = 4;
    let num_events = 10000;
    
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    let producer = disruptor.create_producer();
    
    // Create multiple consumers
    let mut consumers = Vec::new();
    for _ in 0..num_consumers {
        consumers.push(disruptor.create_consumer(vec![]));
    }
    
    let barrier = Arc::new(Barrier::new(num_consumers + 1));
    let total_processed = Arc::new(AtomicUsize::new(0));
    let sum_processed = Arc::new(AtomicU64::new(0));
    
    // Start consumer threads
    let mut handles = Vec::new();
    for consumer in consumers {
        let barrier_clone = Arc::clone(&barrier);
        let total_processed_clone = Arc::clone(&total_processed);
        let sum_processed_clone = Arc::clone(&sum_processed);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait(); // Wait for all threads to start
            
            let mut local_count = 0;
            let mut local_sum = 0u64;
            
            while local_count < num_events {
                let processed = consumer.process_event_batch(100, |_seq, event| {
                    local_sum += event.value;
                    local_count += 1;
                });
                
                if processed == 0 {
                    thread::yield_now();
                }
            }
            
            total_processed_clone.fetch_add(local_count, Ordering::Relaxed);
            sum_processed_clone.fetch_add(local_sum, Ordering::Relaxed);
        });
        
        handles.push(handle);
    }
    
    // Producer thread
    let producer_handle = thread::spawn(move || {
        barrier.wait(); // Wait for all threads to start
        
        for i in 0..num_events * num_consumers {
            let guard = producer.next();
            let sequence = guard.sequence();
            
            unsafe {
                let event = producer.get_mut(sequence);
                event.value = i as u64 + 1; // +1 to avoid zero values
            }
            
            guard.publish();
        }
    });
    
    // Wait for all threads to complete
    producer_handle.join().unwrap();
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify results
    assert_eq!(total_processed.load(Ordering::Relaxed), num_events * num_consumers);
    
    // Calculate expected sum: sum of 1 to (num_events * num_consumers)
    let expected_sum = (1..=(num_events * num_consumers) as u64).sum();
    assert_eq!(sum_processed.load(Ordering::Relaxed), expected_sum);
}

#[test]
fn test_multiple_producers_single_consumer() {
    let capacity = 1024;
    let num_producers = 4;
    let events_per_producer = 2500;
    let total_events = num_producers * events_per_producer;
    
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Multi, // Multi-producer mode
    ));
    
    let consumer = disruptor.create_consumer(vec![]);
    let barrier = Arc::new(Barrier::new(num_producers + 1));
    let events_received = Arc::new(AtomicUsize::new(0));
    let sum_received = Arc::new(AtomicU64::new(0));
    
    // Start producer threads
    let mut producer_handles = Vec::new();
    for producer_id in 0..num_producers {
        let disruptor_clone = Arc::clone(&disruptor);
        let barrier_clone = Arc::clone(&barrier);
        
        let handle = thread::spawn(move || {
            let producer = disruptor_clone.create_producer();
            barrier_clone.wait(); // Wait for all threads to start
            
            for i in 0..events_per_producer {
                let guard = producer.next();
                let sequence = guard.sequence();
                
                unsafe {
                    let event = producer.get_mut(sequence);
                    event.value = (producer_id * 1000 + i) as u64;
                }
                
                guard.publish();
            }
        });
        
        producer_handles.push(handle);
    }
    
    // Consumer thread
    let consumer_handle = thread::spawn(move || {
        barrier.wait(); // Wait for all threads to start
        
        let mut local_count = 0;
        let mut local_sum = 0u64;
        
        while local_count < total_events {
            let processed = consumer.process_event_batch(100, |_seq, event| {
                local_sum += event.value;
                local_count += 1;
            });
            
            if processed == 0 {
                thread::yield_now();
            }
        }
        
        events_received.store(local_count, Ordering::Relaxed);
        sum_received.store(local_sum, Ordering::Relaxed);
    });
    
    // Wait for all threads to complete
    for handle in producer_handles {
        handle.join().unwrap();
    }
    consumer_handle.join().unwrap();
    
    // Verify results
    assert_eq!(events_received.load(Ordering::Relaxed), total_events);
    
    // Calculate expected sum
    let mut expected_sum = 0u64;
    for producer_id in 0..num_producers {
        for i in 0..events_per_producer {
            expected_sum += (producer_id * 1000 + i) as u64;
        }
    }
    assert_eq!(sum_received.load(Ordering::Relaxed), expected_sum);
}

#[test]
fn test_concurrent_producer_consumer_stress() {
    let capacity = 512;
    let num_events = 50000;
    
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    let events_produced = Arc::new(AtomicUsize::new(0));
    let events_consumed = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));
    
    // Producer thread with high frequency
    let producer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let events_produced_clone = Arc::clone(&events_produced);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..num_events {
                let guard = producer.next();
                let sequence = guard.sequence();
                
                unsafe {
                    producer.get_mut(sequence).value = i as u64;
                }
                
                guard.publish();
                events_produced_clone.fetch_add(1, Ordering::Relaxed);
                
                // Add some variability
                if i % 1000 == 0 {
                    thread::yield_now();
                }
            }
        })
    };
    
    // Consumer thread with batch processing
    let consumer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let events_consumed_clone = Arc::clone(&events_consumed);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut total_consumed = 0;
            while total_consumed < num_events {
                let processed = consumer.process_event_batch(50, |_seq, _event| {
                    total_consumed += 1;
                });
                
                events_consumed_clone.store(total_consumed, Ordering::Relaxed);
                
                if processed == 0 {
                    thread::sleep(Duration::from_micros(1));
                }
            }
        })
    };
    
    // Wait for completion
    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
    
    // Verify all events were processed
    assert_eq!(events_produced.load(Ordering::Relaxed), num_events);
    assert_eq!(events_consumed.load(Ordering::Relaxed), num_events);
}

#[test]
fn test_sequence_memory_ordering() {
    use rust_disruptor::sequencer::Sequence;
    
    let sequence = Arc::new(Sequence::new(0));
    let num_threads = 8;
    let increments_per_thread = 1000;
    
    let mut handles = Vec::new();
    
    for _ in 0..num_threads {
        let seq_clone = Arc::clone(&sequence);
        let handle = thread::spawn(move || {
            for _ in 0..increments_per_thread {
                seq_clone.fetch_add(1);
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify final value
    let expected = num_threads * increments_per_thread;
    assert_eq!(sequence.get(), expected);
}

#[test] 
fn test_consumer_dependency_ordering() {
    let capacity = 64;
    let num_events = 1000;
    
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    let producer = disruptor.create_producer();
    
    // Create consumer dependency chain
    let consumer1 = disruptor.create_consumer(vec![]);
    let consumer2 = disruptor.create_consumer(vec![Arc::clone(&consumer1.sequence)]);
    let consumer3 = disruptor.create_consumer(vec![Arc::clone(&consumer2.sequence)]);
    
    let barrier = Arc::new(Barrier::new(4));
    let processing_order = Arc::new(std::sync::Mutex::new(Vec::new()));
    
    // Producer thread
    let producer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..num_events {
                let guard = producer.next();
                let sequence = guard.sequence();
                
                unsafe {
                    producer.get_mut(sequence).value = i as u64;
                }
                
                guard.publish();
            }
        })
    };
    
    // Consumer threads
    let mut consumer_handles = Vec::new();
    
    for (consumer_id, consumer) in [consumer1, consumer2, consumer3].into_iter().enumerate() {
        let barrier_clone = Arc::clone(&barrier);
        let order_clone = Arc::clone(&processing_order);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                let batch_processed = consumer.process_event_batch(10, |seq, _event| {
                    // Record processing order
                    if let Ok(mut order) = order_clone.lock() {
                        order.push((consumer_id, seq));
                    }
                    processed += 1;
                });
                
                if batch_processed == 0 {
                    thread::yield_now();
                }
            }
        });
        
        consumer_handles.push(handle);
    }
    
    // Wait for completion
    producer_handle.join().unwrap();
    for handle in consumer_handles {
        handle.join().unwrap();
    }
    
    // Verify dependency ordering
    let order = processing_order.lock().unwrap();
    let mut last_seq_by_consumer = [0i64; 3];
    
    for &(consumer_id, seq) in order.iter() {
        // Each consumer should process sequences in order
        assert!(seq >= last_seq_by_consumer[consumer_id]);
        last_seq_by_consumer[consumer_id] = seq;
        
        // Dependent consumers should not get ahead
        if consumer_id > 0 {
            let prev_consumer_seq = last_seq_by_consumer[consumer_id - 1];
            assert!(seq <= prev_consumer_seq + 1); // Allow some slack for timing
        }
    }
}

#[test]
fn test_blocking_wait_strategy_thread_safety() {
    let capacity = 64;
    let disruptor = Arc::new(Disruptor::<MyEvent, BlockingWaitStrategy>::new(
        capacity,
        BlockingWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    let events_processed = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));
    
    // Consumer thread - should block waiting for events
    let consumer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let events_clone = Arc::clone(&events_processed);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            // Process events as they become available
            for _ in 0..100 {
                let processed = consumer.process_event_batch(1, |_seq, _event| {
                    events_clone.fetch_add(1, Ordering::Relaxed);
                });
                
                if processed == 0 {
                    thread::sleep(Duration::from_micros(10));
                }
            }
        })
    };
    
    // Producer thread - publishes events with delays
    let producer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..100 {
                let guard = producer.next();
                let sequence = guard.sequence();
                
                unsafe {
                    producer.get_mut(sequence).value = i;
                }
                
                guard.publish();
                
                // Add small delay to test blocking behavior
                thread::sleep(Duration::from_micros(100));
            }
        })
    };
    
    // Wait for completion
    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
    
    assert_eq!(events_processed.load(Ordering::Relaxed), 100);
}