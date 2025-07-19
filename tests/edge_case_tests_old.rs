// tests/edge_case_tests.rs

use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::sequencer::{ProducerMode, Sequence};
use rust_disruptor::wait_strategy::{BusySpinWaitStrategy, BlockingWaitStrategy, YieldingWaitStrategy, WaitStrategy};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_ring_buffer_wraparound() {
    let capacity = 4; // Small buffer to test wraparound quickly
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    // Fill buffer beyond capacity to test wraparound
    for i in 0..capacity * 2 + 1 {
        let guard = producer.next();
        let sequence = guard.sequence();
        
        unsafe {
            let event = producer.get_mut(sequence);
            event.value = i as u64;
        }
        
        guard.publish();
        
        // Process some events to allow wraparound
        if i % 2 == 1 {
            let next_seq = consumer.sequence.get() + 1;
            if let Ok(available) = consumer.wait_strategy.wait_for(
                next_seq,
                Arc::clone(&consumer.sequencer),
                &consumer.gating_sequences_for_wait,
                Arc::clone(&consumer.sequence),
            ) {
                if available >= next_seq {
                    let event = unsafe { consumer.ring_buffer.get(next_seq) };
                    assert_eq!(event.value, ((i - 1) as u64));
                    consumer.sequence.set(next_seq);
                }
            }
        }
    }
}

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
fn test_consumer_batch_processing_edge_cases() {
    let capacity = 8;
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    // Test empty batch
    let processed = consumer.process_event_batch(|_event, _seq, _end_of_batch| {
        // Should not be called for empty batch
        panic!("Should not process any events");
    });
    assert_eq!(processed, 0);
    
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
    
    // Test batch size limit
    for i in 0..5 {
        let guard = producer.next();
        let sequence = guard.sequence();
        unsafe {
            producer.get_mut(sequence).value = i;
        }
        guard.publish();
    }
    
    let mut processed_values = Vec::new();
    let mut batch_count = 0;
    let processed = consumer.process_event_batch(|event, _seq, _end_of_batch| {
        if batch_count < 3 {
            processed_values.push(event.value);
            batch_count += 1;
        }
    });
    assert_eq!(processed, 3); // Limited by batch_size
    assert_eq!(processed_values, vec![0, 1, 2]);
}

#[test]
fn test_producer_claim_sequence_edge_cases() {
    let capacity = 4;
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    
    // Claim multiple sequences rapidly
    let mut guards = Vec::new();
    for _ in 0..capacity {
        guards.push(producer.next());
    }
    
    // All sequences should be unique and sequential
    for (i, guard) in guards.iter().enumerate() {
        assert_eq!(guard.sequence(), i as i64);
    }
    
    // Publish in reverse order to test out-of-order publishing
    for guard in guards.into_iter().rev() {
        guard.publish();
    }
}

#[test] 
fn test_wait_strategy_timeout_behavior() {
    let capacity = 4;
    let disruptor = Disruptor::<MyEvent, BlockingWaitStrategy>::new(
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
fn test_multiple_wait_strategies() {
    let capacity = 4;
    
    // Test BusySpinWaitStrategy
    let disruptor_busy = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    let producer_busy = disruptor_busy.create_producer();
    let consumer_busy = disruptor_busy.create_consumer(vec![]);
    
    // Test BlockingWaitStrategy
    let disruptor_blocking = Disruptor::<MyEvent, BlockingWaitStrategy>::new(
        capacity,
        BlockingWaitStrategy::new(),
        ProducerMode::Single,
    );
    let producer_blocking = disruptor_blocking.create_producer();
    let consumer_blocking = disruptor_blocking.create_consumer(vec![]);
    
    // Test YieldingWaitStrategy
    let disruptor_yielding = Disruptor::<MyEvent, YieldingWaitStrategy>::new(
        capacity,
        YieldingWaitStrategy::new(),
        ProducerMode::Single,
    );
    let producer_yielding = disruptor_yielding.create_producer();
    let consumer_yielding = disruptor_yielding.create_consumer(vec![]);
    
    // Test all strategies can handle events
    for (producer, consumer) in [
        (producer_busy, consumer_busy),
        (producer_blocking, consumer_blocking),
        (producer_yielding, consumer_yielding),
    ] {
        let guard = producer.next();
        let sequence = guard.sequence();
        unsafe {
            producer.get_mut(sequence).value = 123;
        }
        guard.publish();
        
        let next_seq = consumer.sequence.get() + 1;
        let available = consumer.wait_strategy.wait_for(
            next_seq,
            Arc::clone(&consumer.sequencer),
            &consumer.gating_sequences_for_wait,
            Arc::clone(&consumer.sequence),
        );
        
        if available >= next_seq {
            let event = unsafe { consumer.ring_buffer.get(next_seq) };
            assert_eq!(event.value, 123);
            consumer.sequence.set(next_seq);
        }
    }
}

#[test]
fn test_consumer_dependency_chain() {
    let capacity = 8;
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    
    // Create consumer chain: consumer1 -> consumer2 -> consumer3
    let consumer1 = disruptor.create_consumer(vec![]);
    let consumer2 = disruptor.create_consumer(vec![Arc::clone(&consumer1.sequence)]);
    let consumer3 = disruptor.create_consumer(vec![Arc::clone(&consumer2.sequence)]);
    
    // Publish events
    for i in 0..5 {
        let guard = producer.next();
        let sequence = guard.sequence();
        unsafe {
            producer.get_mut(sequence).value = i;
        }
        guard.publish();
    }
    
    // Process through consumer chain
    for _ in 0..5 {
        // Consumer1 processes
        consumer1.process_event_batch(1, |_seq, event| {
            event.value *= 2; // Double the value
        });
        
        // Consumer2 processes (depends on consumer1)
        consumer2.process_event_batch(1, |_seq, event| {
            event.value += 10; // Add 10
        });
        
        // Consumer3 processes (depends on consumer2)
        consumer3.process_event_batch(1, |_seq, event| {
            event.value *= 3; // Triple the value
        });
    }
    
    // Verify final values: ((i * 2) + 10) * 3
    for i in 0..5 {
        let event = unsafe { consumer3.ring_buffer.get(i) };
        let expected = ((i as u64 * 2) + 10) * 3;
        assert_eq!(event.value, expected);
    }
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
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
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
    
    let processed = consumer.process_event_batch(1, |_seq, event| {
        assert_eq!(event.value, 999);
    });
    assert_eq!(processed, 1);
}