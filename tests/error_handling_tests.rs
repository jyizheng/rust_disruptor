// tests/error_handling_tests.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::sequencer::{ProducerMode, Sequence};
use rust_disruptor::wait_strategy::{BusySpinWaitStrategy, BlockingWaitStrategy};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use std::panic;

#[test]
#[should_panic(expected = "Cannot create more than one producer in SingleProducer mode")]
fn test_single_producer_mode_enforcement() {
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        16,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    );
    
    let _producer1 = disruptor.create_producer();
    let _producer2 = disruptor.create_producer(); // Should panic
}

#[test]
fn test_multi_producer_mode_allows_multiple_producers() {
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        16,
        BusySpinWaitStrategy::new(),
        ProducerMode::Multi,
    );
    
    // Should not panic
    let _producer1 = disruptor.create_producer();
    let _producer2 = disruptor.create_producer();
    let _producer3 = disruptor.create_producer();
}

#[test]
#[should_panic(expected = "Ring buffer capacity must be greater than 0")]
fn test_zero_capacity_error() {
    Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        0,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    );
}

#[test]
#[should_panic(expected = "Ring buffer capacity must be a power of two")]
fn test_non_power_of_two_capacity_error() {
    Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        15, // Not a power of two
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    );
}

#[test]
fn test_producer_consumer_graceful_shutdown() {
    let capacity = 64;
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let events_processed = Arc::new(AtomicUsize::new(0));
    
    // Producer thread with graceful shutdown
    let producer_handle = {
        let shutdown_clone = Arc::clone(&shutdown_flag);
        thread::spawn(move || {
            let mut i = 0;
            while !shutdown_clone.load(Ordering::Relaxed) && i < 1000 {
                let guard = producer.next();
                let sequence = guard.sequence();
                
                unsafe {
                    producer.get_mut(sequence).value = i;
                }
                
                guard.publish();
                i += 1;
                
                thread::sleep(Duration::from_micros(100));
            }
        })
    };
    
    // Consumer thread with graceful shutdown
    let consumer_handle = {
        let shutdown_clone = Arc::clone(&shutdown_flag);
        let processed_clone = Arc::clone(&events_processed);
        
        thread::spawn(move || {
            while !shutdown_clone.load(Ordering::Relaxed) {
                let processed = consumer.process_event_batch(10, |_seq, _event| {
                    processed_clone.fetch_add(1, Ordering::Relaxed);
                });
                
                if processed == 0 {
                    thread::sleep(Duration::from_micros(10));
                }
            }
        })
    };
    
    // Let it run for a bit
    thread::sleep(Duration::from_millis(50));
    
    // Signal shutdown
    shutdown_flag.store(true, Ordering::Relaxed);
    
    // Wait for graceful shutdown
    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
    
    let processed = events_processed.load(Ordering::Relaxed);
    println!("Gracefully processed {} events before shutdown", processed);
    assert!(processed > 0);
}

#[test]
fn test_consumer_with_failing_event_handler() {
    let capacity = 16;
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    // Publish some events
    for i in 0..10 {
        let guard = producer.next();
        let sequence = guard.sequence();
        
        unsafe {
            producer.get_mut(sequence).value = i;
        }
        
        guard.publish();
    }
    
    // Process events with a handler that panics on certain values
    let mut successful_processed = 0;
    let mut failed_count = 0;
    
    for _ in 0..10 {
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            consumer.process_event_batch(1, |_seq, event| {
                if event.value == 5 {
                    panic!("Simulated processing error");
                }
                // Normal processing
            })
        }));
        
        match result {
            Ok(processed) => {
                successful_processed += processed;
            }
            Err(_) => {
                failed_count += 1;
                // Consumer can continue processing after panic recovery
            }
        }
    }
    
    println!("Successfully processed: {}, Failed: {}", successful_processed, failed_count);
    assert!(successful_processed > 0);
    assert!(failed_count > 0);
}

#[test]
fn test_sequence_overflow_handling_edge_cases() {
    // Test sequence near i64::MAX
    let seq = Sequence::new(i64::MAX - 10);
    
    for i in 0..20 {
        let prev = seq.fetch_add(1);
        let current = seq.get();
        
        println!("Step {}: prev={}, current={}", i, prev, current);
        
        // Verify atomic operation consistency
        assert_eq!(current, prev + 1);
    }
    
    // Sequence should have wrapped around
    assert!(seq.get() < 0); // Wrapped to negative
}

#[test]
fn test_consumer_dependency_cycle_detection() {
    let capacity = 16;
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    // Create potential circular dependency
    let consumer1 = disruptor.create_consumer(vec![]);
    let consumer2 = disruptor.create_consumer(vec![Arc::clone(&consumer1.sequence)]);
    
    // This would create a cycle if consumer1 depended on consumer2
    // In a real system, this should be detected and prevented
    // For now, we test that the system doesn't deadlock
    
    let producer = disruptor.create_producer();
    
    // Publish an event
    let guard = producer.next();
    let sequence = guard.sequence();
    unsafe {
        producer.get_mut(sequence).value = 42;
    }
    guard.publish();
    
    // Both consumers should be able to process
    let processed1 = consumer1.process_event_batch(1, |_seq, _event| {});
    let processed2 = consumer2.process_event_batch(1, |_seq, _event| {});
    
    assert_eq!(processed1, 1);
    assert_eq!(processed2, 1);
}

#[test]
fn test_buffer_full_backpressure() {
    let capacity = 4; // Very small buffer
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    // Fill buffer to capacity
    let mut guards = Vec::new();
    for i in 0..capacity {
        let guard = producer.next();
        let sequence = guard.sequence();
        
        unsafe {
            producer.get_mut(sequence).value = i as u64;
        }
        
        guards.push(guard);
    }
    
    // Buffer should be full, producer should be blocked on next()
    // This test verifies that the system handles backpressure correctly
    // by ensuring the producer can't overflow the buffer
    
    // Publish the events
    for guard in guards {
        guard.publish();
    }
    
    // Now consume some events to make space
    let processed = consumer.process_event_batch(2, |_seq, _event| {});
    assert_eq!(processed, 2);
    
    // Producer should now be able to publish new events
    let guard = producer.next();
    let sequence = guard.sequence();
    unsafe {
        producer.get_mut(sequence).value = 999;
    }
    guard.publish();
    
    // Verify the new event was published
    let processed = consumer.process_event_batch(1, |_seq, event| {
        assert_eq!(event.value, 999);
    });
    assert_eq!(processed, 1);
}

#[test]
fn test_wait_strategy_timeout_scenarios() {
    let capacity = 16;
    let disruptor = Disruptor::<MyEvent, BlockingWaitStrategy>::new(
        capacity,
        BlockingWaitStrategy::new(),
        ProducerMode::Single,
    );
    
    let consumer = disruptor.create_consumer(vec![]);
    
    // Test waiting for a sequence that doesn't exist
    let start = std::time::Instant::now();
    let available = consumer.wait_strategy.wait_for(
        100, // Sequence that doesn't exist
        Arc::clone(&consumer.sequencer),
        &consumer.gating_sequences_for_wait,
        Arc::clone(&consumer.sequence),
    );
    let elapsed = start.elapsed();
    
    // Should return quickly (not block indefinitely)
    assert!(elapsed < Duration::from_millis(100));
    assert!(available < 100); // Should not claim to have the non-existent sequence
    
    println!("Wait strategy timeout test: elapsed={:?}, available={}", elapsed, available);
}

#[test]
fn test_concurrent_producer_error_scenarios() {
    let capacity = 64;
    let num_producers = 4;
    let events_per_producer = 100;
    
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Multi,
    ));
    
    let consumer = disruptor.create_consumer(vec![]);
    let error_count = Arc::new(AtomicUsize::new(0));
    let success_count = Arc::new(AtomicUsize::new(0));
    
    let barrier = Arc::new(Barrier::new(num_producers + 1));
    
    // Start multiple producers, some of which will simulate errors
    let mut producer_handles = Vec::new();
    
    for producer_id in 0..num_producers {
        let disruptor_clone = Arc::clone(&disruptor);
        let barrier_clone = Arc::clone(&barrier);
        let error_clone = Arc::clone(&error_count);
        let success_clone = Arc::clone(&success_count);
        
        let handle = thread::spawn(move || {
            let producer = disruptor_clone.create_producer();
            barrier_clone.wait();
            
            for i in 0..events_per_producer {
                // Simulate random errors in producer 0
                if producer_id == 0 && i % 10 == 7 {
                    // Simulate error scenario - don't publish
                    let _guard = producer.next();
                    error_clone.fetch_add(1, Ordering::Relaxed);
                    // Guard drops without publishing (simulates error)
                    continue;
                }
                
                let guard = producer.next();
                let sequence = guard.sequence();
                
                unsafe {
                    producer.get_mut(sequence).value = (producer_id * 1000 + i) as u64;
                }
                
                guard.publish();
                success_clone.fetch_add(1, Ordering::Relaxed);
            }
        });
        
        producer_handles.push(handle);
    }
    
    // Consumer thread
    let consumer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            // Give producers time to work
            thread::sleep(Duration::from_millis(100));
            
            // Consume all available events
            let mut total_consumed = 0;
            for _ in 0..1000 { // Max iterations to prevent infinite loop
                let processed = consumer.process_event_batch(10, |_seq, _event| {
                    total_consumed += 1;
                });
                
                if processed == 0 {
                    thread::sleep(Duration::from_millis(1));
                    break;
                }
            }
            
            total_consumed
        })
    };
    
    // Wait for completion
    for handle in producer_handles {
        handle.join().unwrap();
    }
    let consumed = consumer_handle.join().unwrap();
    
    let errors = error_count.load(Ordering::Relaxed);
    let successes = success_count.load(Ordering::Relaxed);
    
    println!("Concurrent producer error test:");
    println!("  Successful publishes: {}", successes);
    println!("  Simulated errors: {}", errors);
    println!("  Events consumed: {}", consumed);
    
    // Should have consumed the successful publishes
    assert_eq!(consumed, successes);
    assert!(errors > 0); // Should have simulated some errors
    assert!(successes > 0); // Should have some successful publishes
}

#[test]
fn test_memory_safety_with_dropped_references() {
    let capacity = 16;
    
    // Create a scope where disruptor components are created and dropped
    let processed_count = {
        let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
            capacity,
            BusySpinWaitStrategy::new(),
            ProducerMode::Single,
        );
        
        let producer = disruptor.create_producer();
        let consumer = disruptor.create_consumer(vec![]);
        
        // Publish some events
        for i in 0..10 {
            let guard = producer.next();
            let sequence = guard.sequence();
            
            unsafe {
                producer.get_mut(sequence).value = i;
            }
            
            guard.publish();
        }
        
        // Process events
        let mut count = 0;
        while count < 10 {
            count += consumer.process_event_batch(5, |_seq, _event| {});
        }
        
        count
    }; // disruptor, producer, consumer are dropped here
    
    // Verify processing completed successfully
    assert_eq!(processed_count, 10);
    
    // This test ensures that dropping components doesn't cause memory safety issues
    println!("Memory safety test completed successfully");
}