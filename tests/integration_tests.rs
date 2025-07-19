// tests/integration_tests.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::sequencer::ProducerMode;
use rust_disruptor::wait_strategy::{BusySpinWaitStrategy, BlockingWaitStrategy, YieldingWaitStrategy};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_end_to_end_single_producer_single_consumer() {
    let capacity = 1024;
    let num_events = 100000;
    
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    let start_time = Instant::now();
    
    // Publish events
    for i in 0..num_events {
        let guard = producer.next();
        let sequence = guard.sequence();
        
        unsafe {
            let event = producer.get_mut(sequence);
            event.value = i as u64;
        }
        
        guard.publish();
    }
    
    let publish_time = start_time.elapsed();
    
    // Consume events
    let mut consumed_count = 0;
    let mut sum = 0u64;
    
    while consumed_count < num_events {
        consumed_count += consumer.process_event_batch(100, |_seq, event| {
            sum += event.value;
        });
    }
    
    let total_time = start_time.elapsed();
    
    // Verify results
    assert_eq!(consumed_count, num_events);
    let expected_sum = (0..num_events as u64).sum();
    assert_eq!(sum, expected_sum);
    
    println!("End-to-end test completed:");
    println!("  Events: {}", num_events);
    println!("  Publish time: {:?}", publish_time);
    println!("  Total time: {:?}", total_time);
    println!("  Throughput: {:.2} events/sec", num_events as f64 / total_time.as_secs_f64());
}

#[test]
fn test_producer_consumer_pipeline() {
    let capacity = 256;
    let num_events = 10000;
    
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    // Create processing pipeline: Stage1 -> Stage2 -> Stage3
    let stage1 = disruptor.create_consumer(vec![]);
    let stage2 = disruptor.create_consumer(vec![Arc::clone(&stage1.sequence)]);
    let stage3 = disruptor.create_consumer(vec![Arc::clone(&stage2.sequence)]);
    
    let producer = disruptor.create_producer();
    let results = Arc::new(std::sync::Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(4));
    
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
    
    // Stage 1: Multiply by 2
    let stage1_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                processed += stage1.process_event_batch(50, |_seq, event| {
                    event.value *= 2;
                });
                
                if processed % 1000 == 0 {
                    thread::yield_now();
                }
            }
        })
    };
    
    // Stage 2: Add 100
    let stage2_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                processed += stage2.process_event_batch(50, |_seq, event| {
                    event.value += 100;
                });
                
                if processed % 1000 == 0 {
                    thread::yield_now();
                }
            }
        })
    };
    
    // Stage 3: Collect results
    let stage3_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let results_clone = Arc::clone(&results);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                processed += stage3.process_event_batch(50, |_seq, event| {
                    if let Ok(mut results) = results_clone.lock() {
                        results.push(event.value);
                    }
                });
                
                if processed % 1000 == 0 {
                    thread::yield_now();
                }
            }
        })
    };
    
    // Wait for all stages to complete
    producer_handle.join().unwrap();
    stage1_handle.join().unwrap();
    stage2_handle.join().unwrap();
    stage3_handle.join().unwrap();
    
    // Verify pipeline results
    let final_results = results.lock().unwrap();
    assert_eq!(final_results.len(), num_events);
    
    for (i, &value) in final_results.iter().enumerate() {
        let expected = (i as u64 * 2) + 100; // (original * 2) + 100
        assert_eq!(value, expected, "Mismatch at index {}", i);
    }
}

#[test]
fn test_fan_out_pattern() {
    let capacity = 512;
    let num_events = 5000;
    
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    let producer = disruptor.create_producer();
    
    // Create multiple independent consumers (fan-out)
    let consumer1 = disruptor.create_consumer(vec![]); // Logger
    let consumer2 = disruptor.create_consumer(vec![]); // Metrics
    let consumer3 = disruptor.create_consumer(vec![]); // Audit
    
    let logged_events = Arc::new(AtomicUsize::new(0));
    let metrics_sum = Arc::new(AtomicU64::new(0));
    let audit_count = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(4));
    
    // Producer
    let producer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..num_events {
                let guard = producer.next();
                let sequence = guard.sequence();
                
                unsafe {
                    producer.get_mut(sequence).value = (i + 1) as u64;
                }
                
                guard.publish();
            }
        })
    };
    
    // Logger consumer
    let logger_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let logged_clone = Arc::clone(&logged_events);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                processed += consumer1.process_event_batch(25, |_seq, _event| {
                    logged_clone.fetch_add(1, Ordering::Relaxed);
                });
                
                thread::yield_now();
            }
        })
    };
    
    // Metrics consumer
    let metrics_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let metrics_clone = Arc::clone(&metrics_sum);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                processed += consumer2.process_event_batch(25, |_seq, event| {
                    metrics_clone.fetch_add(event.value, Ordering::Relaxed);
                });
                
                thread::yield_now();
            }
        })
    };
    
    // Audit consumer (only processes even-valued events)
    let audit_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let audit_clone = Arc::clone(&audit_count);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                processed += consumer3.process_event_batch(25, |_seq, event| {
                    if event.value % 2 == 0 {
                        audit_clone.fetch_add(1, Ordering::Relaxed);
                    }
                });
                
                thread::yield_now();
            }
        })
    };
    
    // Wait for completion
    producer_handle.join().unwrap();
    logger_handle.join().unwrap();
    metrics_handle.join().unwrap();
    audit_handle.join().unwrap();
    
    // Verify fan-out results
    assert_eq!(logged_events.load(Ordering::Relaxed), num_events);
    
    let expected_sum = (1..=num_events as u64).sum();
    assert_eq!(metrics_sum.load(Ordering::Relaxed), expected_sum);
    
    let expected_audit_count = (1..=num_events).filter(|&i| i % 2 == 0).count();
    assert_eq!(audit_count.load(Ordering::Relaxed), expected_audit_count);
}

#[test]
fn test_diamond_dependency_pattern() {
    let capacity = 256;
    let num_events = 1000;
    
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    let producer = disruptor.create_producer();
    
    // Diamond pattern: Stage1 -> (Stage2A, Stage2B) -> Stage3
    let stage1 = disruptor.create_consumer(vec![]);
    let stage2a = disruptor.create_consumer(vec![Arc::clone(&stage1.sequence)]);
    let stage2b = disruptor.create_consumer(vec![Arc::clone(&stage1.sequence)]);
    let stage3 = disruptor.create_consumer(vec![
        Arc::clone(&stage2a.sequence),
        Arc::clone(&stage2b.sequence),
    ]);
    
    let final_results = Arc::new(std::sync::Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(5));
    
    // Producer
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
    
    // Stage 1: Initialize processing
    let stage1_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                processed += stage1.process_event_batch(20, |_seq, event| {
                    event.value += 1000; // Base offset
                });
                
                thread::yield_now();
            }
        })
    };
    
    // Stage 2A: Path A processing
    let stage2a_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                processed += stage2a.process_event_batch(20, |_seq, event| {
                    event.value *= 2; // Double
                });
                
                thread::yield_now();
            }
        })
    };
    
    // Stage 2B: Path B processing
    let stage2b_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                processed += stage2b.process_event_batch(20, |_seq, event| {
                    event.value += 500; // Add offset
                });
                
                thread::yield_now();
            }
        })
    };
    
    // Stage 3: Final processing (depends on both 2A and 2B)
    let stage3_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let results_clone = Arc::clone(&final_results);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_events {
                processed += stage3.process_event_batch(20, |_seq, event| {
                    if let Ok(mut results) = results_clone.lock() {
                        results.push(event.value);
                    }
                });
                
                thread::yield_now();
            }
        })
    };
    
    // Wait for completion
    producer_handle.join().unwrap();
    stage1_handle.join().unwrap();
    stage2a_handle.join().unwrap();
    stage2b_handle.join().unwrap();
    stage3_handle.join().unwrap();
    
    // Verify diamond pattern results
    let results = final_results.lock().unwrap();
    assert_eq!(results.len(), num_events);
    
    for (i, &value) in results.iter().enumerate() {
        // Original: i
        // Stage1: i + 1000
        // Stage2A: (i + 1000) * 2
        // Stage2B: (i + 1000) * 2 + 500  (Stage2B processes after 2A)
        let expected = ((i as u64 + 1000) * 2) + 500;
        assert_eq!(value, expected, "Mismatch at index {}", i);
    }
}

#[test]
fn test_backpressure_handling() {
    let capacity = 32; // Small buffer to create backpressure
    let num_events = 1000;
    
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
    
    // Fast producer
    let producer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let produced_clone = Arc::clone(&events_produced);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let start = Instant::now();
            
            for i in 0..num_events {
                let guard = producer.next(); // This should block when buffer is full
                let sequence = guard.sequence();
                
                unsafe {
                    producer.get_mut(sequence).value = i as u64;
                }
                
                guard.publish();
                produced_clone.fetch_add(1, Ordering::Relaxed);
            }
            
            println!("Producer completed in {:?}", start.elapsed());
        })
    };
    
    // Slow consumer (with delays)
    let consumer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let consumed_clone = Arc::clone(&events_consumed);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            // Add initial delay to create backpressure
            thread::sleep(Duration::from_millis(10));
            
            let mut total_consumed = 0;
            while total_consumed < num_events {
                let processed = consumer.process_event_batch(5, |_seq, _event| {
                    // Simulate slow processing
                    thread::sleep(Duration::from_micros(50));
                    total_consumed += 1;
                });
                
                consumed_clone.store(total_consumed, Ordering::Relaxed);
                
                if processed == 0 {
                    thread::yield_now();
                }
            }
        })
    };
    
    // Wait for completion
    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
    
    // Verify backpressure was handled correctly
    assert_eq!(events_produced.load(Ordering::Relaxed), num_events);
    assert_eq!(events_consumed.load(Ordering::Relaxed), num_events);
}

#[test]
fn test_real_world_order_processing() {
    #[derive(Default, Clone, Copy)]
    struct OrderEvent {
        order_id: u64,
        quantity: u32,
        price: u64, // Price in cents
        processed: bool,
        validated: bool,
        risk_checked: bool,
    }
    
    impl rust_disruptor::event::Event for OrderEvent {
        fn default() -> Self {
            OrderEvent {
                order_id: 0,
                quantity: 0,
                price: 0,
                processed: false,
                validated: false,
                risk_checked: false,
            }
        }
    }
    
    let capacity = 1024;
    let num_orders = 5000;
    
    let disruptor = Arc::new(Disruptor::<OrderEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    let producer = disruptor.create_producer();
    
    // Order processing pipeline
    let validator = disruptor.create_consumer(vec![]);
    let risk_checker = disruptor.create_consumer(vec![Arc::clone(&validator.sequence)]);
    let processor = disruptor.create_consumer(vec![Arc::clone(&risk_checker.sequence)]);
    
    let processed_orders = Arc::new(AtomicUsize::new(0));
    let total_value = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(4));
    
    // Order generator
    let producer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..num_orders {
                let guard = producer.next();
                let sequence = guard.sequence();
                
                unsafe {
                    let order = producer.get_mut(sequence);
                    order.order_id = i as u64 + 1;
                    order.quantity = (i % 100 + 1) as u32;
                    order.price = ((i % 1000) + 100) as u64;
                }
                
                guard.publish();
            }
        })
    };
    
    // Validator
    let validator_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_orders {
                processed += validator.process_event_batch(50, |_seq, order| {
                    // Validate order (simple validation)
                    order.validated = order.quantity > 0 && order.price > 0;
                });
                
                thread::yield_now();
            }
        })
    };
    
    // Risk checker
    let risk_handle = {
        let barrier_clone = Arc::clone(&barrier);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_orders {
                processed += risk_checker.process_event_batch(50, |_seq, order| {
                    // Risk check (reject high-value orders)
                    let order_value = order.quantity as u64 * order.price;
                    order.risk_checked = order.validated && order_value < 50000; // Max $500
                });
                
                thread::yield_now();
            }
        })
    };
    
    // Order processor
    let processor_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let processed_clone = Arc::clone(&processed_orders);
        let value_clone = Arc::clone(&total_value);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut processed = 0;
            while processed < num_orders {
                processed += processor.process_event_batch(50, |_seq, order| {
                    if order.validated && order.risk_checked {
                        order.processed = true;
                        let order_value = order.quantity as u64 * order.price;
                        
                        processed_clone.fetch_add(1, Ordering::Relaxed);
                        value_clone.fetch_add(order_value, Ordering::Relaxed);
                    }
                });
                
                thread::yield_now();
            }
        })
    };
    
    // Wait for completion
    producer_handle.join().unwrap();
    validator_handle.join().unwrap();
    risk_handle.join().unwrap();
    processor_handle.join().unwrap();
    
    // Verify order processing results
    let orders_processed = processed_orders.load(Ordering::Relaxed);
    let total_order_value = total_value.load(Ordering::Relaxed);
    
    println!("Order processing completed:");
    println!("  Total orders: {}", num_orders);
    println!("  Processed orders: {}", orders_processed);
    println!("  Total value: ${:.2}", total_order_value as f64 / 100.0);
    println!("  Success rate: {:.1}%", orders_processed as f64 / num_orders as f64 * 100.0);
    
    // Should have processed most orders (those under risk limit)
    assert!(orders_processed > 0);
    assert!(orders_processed <= num_orders);
    assert!(total_order_value > 0);
}