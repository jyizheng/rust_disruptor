// tests/edge_case_tests_simple.rs

use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::sequencer::{ProducerMode, Sequence};
use rust_disruptor::wait_strategy::{BusySpinWaitStrategy, BlockingWaitStrategy, WaitStrategy};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_sequence_overflow_handling() {
    let seq = Sequence::new(i64::MAX - 1);
    
    // Test near overflow
    assert_eq!(seq.get(), i64::MAX - 1);
    
    let prev = seq.fetch_add(1);
    assert_eq!(prev, i64::MAX - 1);
    assert_eq!(seq.get(), i64::MAX);
    
    // Test overflow wraparound
    let prev = seq.fetch_add(1);
    assert_eq!(prev, i64::MAX);
    assert_eq!(seq.get(), i64::MIN); // Wraps around
}

#[test]
fn test_consumer_batch_processing_empty() {
    let capacity = 8;
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    
    let consumer = disruptor.create_consumer(vec![]);
    
    // Test empty batch
    let processed = consumer.process_event_batch(|_event, _seq, _end_of_batch| {
        panic!("Should not process any events");
    });
    assert_eq!(processed, 0);
}

#[test]
fn test_consumer_batch_processing_single_event() {
    let capacity = 8;
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    // Add single event
    let guard = producer.next();
    let sequence = guard.sequence();
    unsafe {
        producer.get_mut(sequence).value = 42;
    }
    guard.publish();
    
    // Test single event batch
    let mut count = 0;
    let processed = consumer.process_event_batch(|event, _seq, _end_of_batch| {
        assert_eq!(event.value, 42);
        count += 1;
    });
    assert_eq!(processed, 1);
    assert_eq!(count, 1);
}

#[test]
fn test_wait_strategy_timeout_behavior() {
    let capacity = 4;
    let mut disruptor = Disruptor::<MyEvent, BlockingWaitStrategy>::new(
        capacity,
        BlockingWaitStrategy::new(),
        ProducerMode::Single,
    );
    
    let consumer = disruptor.create_consumer(vec![]);
    
    // Test wait for non-existent sequence (should timeout)
    let start = std::time::Instant::now();
    let result = consumer.wait_strategy.wait_for(
        1, // Sequence that doesn't exist
        Arc::clone(&consumer.sequencer),
        &consumer.gating_sequences_for_wait,
        Arc::clone(&consumer.sequence),
    );
    let elapsed = start.elapsed();
    
    // Should return quickly for non-blocking behavior or after timeout
    assert!(elapsed < Duration::from_millis(100));
    assert!(result < 1); // Should not find the sequence
}

#[test]
fn test_busyspin_wait_strategy() {
    let capacity = 4;
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    let guard = producer.next();
    let sequence = guard.sequence();
    unsafe {
        producer.get_mut(sequence).value = 123;
    }
    guard.publish();
    
    let processed = consumer.process_event_batch(|event, _seq, _end_of_batch| {
        assert_eq!(event.value, 123);
    });
    assert_eq!(processed, 1);
}

#[test]
fn test_consumer_dependency_simple() {
    let capacity = 8;
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    
    // Create consumer chain: consumer1 -> consumer2
    let consumer1 = disruptor.create_consumer(vec![]);
    let consumer2 = disruptor.create_consumer(vec![Arc::clone(&consumer1.sequence)]);
    
    // Publish event
    let guard = producer.next();
    let sequence = guard.sequence();
    unsafe {
        producer.get_mut(sequence).value = 5;
    }
    guard.publish();
    
    // Consumer1 processes first
    let processed1 = consumer1.process_event_batch(|_event, _seq, _end_of_batch| {
        // Read-only processing
    });
    assert_eq!(processed1, 1);
    
    // Consumer2 processes after consumer1
    let processed2 = consumer2.process_event_batch(|event, _seq, _end_of_batch| {
        assert_eq!(event.value, 5);
    });
    assert_eq!(processed2, 1);
}

#[test]
#[should_panic(expected = "Ring buffer capacity must be a power of two")]
fn test_invalid_buffer_size_panic() {
    Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        7, // Not a power of two
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
}

#[test]
#[should_panic(expected = "Ring buffer capacity must be greater than 0")]
fn test_zero_buffer_size_panic() {
    Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        0,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
}

#[test]
fn test_large_buffer_size() {
    let capacity = 1024 * 1024; // 1MB buffer
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    // Test that large buffer works correctly
    let guard = producer.next();
    let sequence = guard.sequence();
    unsafe {
        producer.get_mut(sequence).value = 999;
    }
    guard.publish();
    
    let processed = consumer.process_event_batch(|event, _seq, _end_of_batch| {
        assert_eq!(event.value, 999);
    });
    assert_eq!(processed, 1);
}

#[test]
fn test_multiple_events_batch_processing() {
    let capacity = 16;
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    // Publish multiple events
    for i in 0..5 {
        let guard = producer.next();
        let sequence = guard.sequence();
        unsafe {
            producer.get_mut(sequence).value = i;
        }
        guard.publish();
    }
    
    // Process all available events
    let mut processed_values = Vec::new();
    let processed = consumer.process_event_batch(|event, _seq, _end_of_batch| {
        processed_values.push(event.value);
    });
    
    assert_eq!(processed, 5);
    assert_eq!(processed_values, vec![0, 1, 2, 3, 4]);
}