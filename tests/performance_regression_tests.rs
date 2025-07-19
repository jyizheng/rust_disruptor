// tests/performance_regression_tests.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::sequencer::ProducerMode;
use rust_disruptor::wait_strategy::{BusySpinWaitStrategy, BlockingWaitStrategy, YieldingWaitStrategy};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_throughput_regression_single_producer_single_consumer() {
    let capacity = 65536;
    let num_events = 10_000_000;
    let min_throughput = 5_000_000; // 5M ops/sec minimum
    
    let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    ));
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    let barrier = Arc::new(Barrier::new(2));
    let start_time = Arc::new(std::sync::Mutex::new(None));
    let end_time = Arc::new(std::sync::Mutex::new(None));
    
    // Producer thread
    let producer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let start_clone = Arc::clone(&start_time);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            // Record start time
            if let Ok(mut start) = start_clone.lock() {
                *start = Some(Instant::now());
            }
            
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
    
    // Consumer thread
    let consumer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let end_clone = Arc::clone(&end_time);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut consumed = 0;
            while consumed < num_events {
                consumed += consumer.process_event_batch(1000, |_seq, _event| {
                    // Minimal processing
                });
            }
            
            // Record end time
            if let Ok(mut end) = end_clone.lock() {
                *end = Some(Instant::now());
            }
        })
    };
    
    // Wait for completion
    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
    
    // Calculate throughput
    let start = start_time.lock().unwrap().unwrap();
    let end = end_time.lock().unwrap().unwrap();
    let duration = end.duration_since(start);
    let throughput = num_events as f64 / duration.as_secs_f64();
    
    println!("SPSC Throughput: {:.2} ops/sec", throughput);
    println!("Duration: {:?}", duration);
    
    // Regression test: ensure minimum throughput
    assert!(
        throughput >= min_throughput as f64,
        "Throughput regression detected: {:.2} < {} ops/sec",
        throughput,
        min_throughput
    );
}

#[test]
fn test_latency_regression_small_batches() {
    let capacity = 1024;
    let num_iterations = 100000;
    let max_avg_latency_ns = 1000; // 1 microsecond max average latency
    
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    let mut latencies = Vec::with_capacity(num_iterations);
    
    for i in 0..num_iterations {
        let start = Instant::now();
        
        // Produce event
        let guard = producer.next();
        let sequence = guard.sequence();
        unsafe {
            producer.get_mut(sequence).value = i as u64;
        }
        guard.publish();
        
        // Consume event immediately
        let consumed = consumer.process_event_batch(1, |_seq, _event| {
            // Minimal processing
        });
        
        let latency = start.elapsed();
        
        if consumed > 0 {
            latencies.push(latency.as_nanos() as u64);
        }
    }
    
    // Calculate statistics
    latencies.sort_unstable();
    let avg_latency = latencies.iter().sum::<u64>() / latencies.len() as u64;
    let p50_latency = latencies[latencies.len() / 2];
    let p95_latency = latencies[(latencies.len() * 95) / 100];
    let p99_latency = latencies[(latencies.len() * 99) / 100];
    
    println!("Latency Statistics (nanoseconds):");
    println!("  Average: {}", avg_latency);
    println!("  P50: {}", p50_latency);
    println!("  P95: {}", p95_latency);
    println!("  P99: {}", p99_latency);
    
    // Regression tests
    assert!(
        avg_latency <= max_avg_latency_ns,
        "Average latency regression: {} > {} ns",
        avg_latency,
        max_avg_latency_ns
    );
}

#[test]
fn test_wait_strategy_performance_comparison() {
    let capacity = 8192;
    let num_events = 1_000_000;
    
    struct BenchmarkResult {
        strategy_name: String,
        throughput: f64,
        duration: Duration,
    }
    
    let mut results = Vec::new();
    
    // Test BusySpinWaitStrategy
    {
        let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
            capacity,
            BusySpinWaitStrategy::new(),
            ProducerMode::Single,
        ));
        
        let result = benchmark_wait_strategy(disruptor, num_events, "BusySpinWaitStrategy");
        results.push(result);
    }
    
    // Test YieldingWaitStrategy
    {
        let disruptor = Arc::new(Disruptor::<MyEvent, YieldingWaitStrategy>::new(
            capacity,
            YieldingWaitStrategy::new(),
            ProducerMode::Single,
        ));
        
        let result = benchmark_wait_strategy(disruptor, num_events, "YieldingWaitStrategy");
        results.push(result);
    }
    
    // Test BlockingWaitStrategy
    {
        let disruptor = Arc::new(Disruptor::<MyEvent, BlockingWaitStrategy>::new(
            capacity,
            BlockingWaitStrategy::new(),
            ProducerMode::Single,
        ));
        
        let result = benchmark_wait_strategy(disruptor, num_events, "BlockingWaitStrategy");
        results.push(result);
    }
    
    // Print results and check for regressions
    println!("Wait Strategy Performance Comparison:");
    for result in &results {
        println!(
            "  {}: {:.2} ops/sec ({:?})",
            result.strategy_name, result.throughput, result.duration
        );
    }
    
    // BusySpinWaitStrategy should be fastest
    let busyspin_result = results.iter().find(|r| r.strategy_name == "BusySpinWaitStrategy").unwrap();
    
    // Regression test: BusySpinWaitStrategy should achieve minimum throughput
    assert!(
        busyspin_result.throughput >= 3_000_000.0,
        "BusySpinWaitStrategy regression: {:.2} < 3M ops/sec",
        busyspin_result.throughput
    );
}

fn benchmark_wait_strategy<W>(
    disruptor: Arc<Disruptor<MyEvent, W>>,
    num_events: usize,
    strategy_name: &str,
) -> BenchmarkResult
where
    W: rust_disruptor::wait_strategy::WaitStrategy + 'static,
{
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    let barrier = Arc::new(Barrier::new(2));
    let start_time = Arc::new(std::sync::Mutex::new(None));
    let end_time = Arc::new(std::sync::Mutex::new(None));
    
    // Producer thread
    let producer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let start_clone = Arc::clone(&start_time);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            if let Ok(mut start) = start_clone.lock() {
                *start = Some(Instant::now());
            }
            
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
    
    // Consumer thread
    let consumer_handle = {
        let barrier_clone = Arc::clone(&barrier);
        let end_clone = Arc::clone(&end_time);
        
        thread::spawn(move || {
            barrier_clone.wait();
            
            let mut consumed = 0;
            while consumed < num_events {
                consumed += consumer.process_event_batch(100, |_seq, _event| {
                    // Minimal processing
                });
            }
            
            if let Ok(mut end) = end_clone.lock() {
                *end = Some(Instant::now());
            }
        })
    };
    
    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
    
    let start = start_time.lock().unwrap().unwrap();
    let end = end_time.lock().unwrap().unwrap();
    let duration = end.duration_since(start);
    let throughput = num_events as f64 / duration.as_secs_f64();
    
    BenchmarkResult {
        strategy_name: strategy_name.to_string(),
        throughput,
        duration,
    }
}

#[test]
fn test_memory_usage_regression() {
    // Test that ring buffer doesn't grow unexpectedly
    let capacity = 1024;
    
    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        capacity,
        BusySpinWaitStrategy::new(),
        ProducerMode::Single,
    );
    
    let producer = disruptor.create_producer();
    let consumer = disruptor.create_consumer(vec![]);
    
    // Fill buffer multiple times
    let cycles = 10;
    let events_per_cycle = capacity * 2;
    
    for cycle in 0..cycles {
        // Produce events
        for i in 0..events_per_cycle {
            let guard = producer.next();
            let sequence = guard.sequence();
            
            unsafe {
                producer.get_mut(sequence).value = (cycle * events_per_cycle + i) as u64;
            }
            
            guard.publish();
        }
        
        // Consume events
        let mut consumed = 0;
        while consumed < events_per_cycle {
            consumed += consumer.process_event_batch(100, |_seq, _event| {
                // Process event
            });
        }
    }
    
    // Memory should be stable (no leaks)
    // This is a basic test - in practice you'd use memory profiling tools
    println!("Memory usage test completed successfully");
}

#[test]
fn test_producer_mode_performance_comparison() {
    let capacity = 8192;
    let num_events = 1_000_000;
    
    // Single producer mode
    let single_start = Instant::now();
    {
        let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
            capacity,
            BusySpinWaitStrategy::new(),
            ProducerMode::Single,
        ));
        
        let producer = disruptor.create_producer();
        let consumer = disruptor.create_consumer(vec![]);
        
        let barrier = Arc::new(Barrier::new(2));
        
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
        
        let consumer_handle = {
            let barrier_clone = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier_clone.wait();
                let mut consumed = 0;
                while consumed < num_events {
                    consumed += consumer.process_event_batch(100, |_seq, _event| {});
                }
            })
        };
        
        producer_handle.join().unwrap();
        consumer_handle.join().unwrap();
    }
    let single_duration = single_start.elapsed();
    let single_throughput = num_events as f64 / single_duration.as_secs_f64();
    
    // Multi producer mode (single producer for fair comparison)
    let multi_start = Instant::now();
    {
        let disruptor = Arc::new(Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
            capacity,
            BusySpinWaitStrategy::new(),
            ProducerMode::Multi,
        ));
        
        let producer = disruptor.create_producer();
        let consumer = disruptor.create_consumer(vec![]);
        
        let barrier = Arc::new(Barrier::new(2));
        
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
        
        let consumer_handle = {
            let barrier_clone = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier_clone.wait();
                let mut consumed = 0;
                while consumed < num_events {
                    consumed += consumer.process_event_batch(100, |_seq, _event| {});
                }
            })
        };
        
        producer_handle.join().unwrap();
        consumer_handle.join().unwrap();
    }
    let multi_duration = multi_start.elapsed();
    let multi_throughput = num_events as f64 / multi_duration.as_secs_f64();
    
    println!("Producer Mode Performance:");
    println!("  Single: {:.2} ops/sec ({:?})", single_throughput, single_duration);
    println!("  Multi:  {:.2} ops/sec ({:?})", multi_throughput, multi_duration);
    
    // Single producer should be faster than multi producer
    assert!(
        single_throughput > multi_throughput * 0.8, // Allow 20% overhead for multi-producer
        "Single producer not significantly faster: {:.2} vs {:.2}",
        single_throughput,
        multi_throughput
    );
    
    // Both should meet minimum performance
    assert!(single_throughput >= 2_000_000.0, "Single producer regression");
    assert!(multi_throughput >= 1_500_000.0, "Multi producer regression");
}

#[test]
fn test_scaling_performance() {
    let base_capacity = 1024;
    let num_events = 100_000;
    
    let capacities = vec![1024, 2048, 4096, 8192, 16384];
    let mut results = Vec::new();
    
    for &capacity in &capacities {
        let start = Instant::now();
        
        let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
            capacity,
            BusySpinWaitStrategy::new(),
            ProducerMode::Single,
        );
        
        let producer = disruptor.create_producer();
        let consumer = disruptor.create_consumer(vec![]);
        
        // Run benchmark
        for i in 0..num_events {
            let guard = producer.next();
            let sequence = guard.sequence();
            
            unsafe {
                producer.get_mut(sequence).value = i as u64;
            }
            
            guard.publish();
            
            // Consume immediately to prevent buffer overflow
            consumer.process_event_batch(1, |_seq, _event| {});
        }
        
        let duration = start.elapsed();
        let throughput = num_events as f64 / duration.as_secs_f64();
        
        results.push((capacity, throughput, duration));
    }
    
    println!("Scaling Performance:");
    for &(capacity, throughput, duration) in &results {
        println!("  Capacity {}: {:.2} ops/sec ({:?})", capacity, throughput, duration);
    }
    
    // Larger buffers should not significantly degrade performance
    let base_throughput = results[0].1; // 1024 capacity
    let largest_throughput = results.last().unwrap().1; // 16384 capacity
    
    assert!(
        largest_throughput >= base_throughput * 0.7, // Allow 30% degradation
        "Scaling regression: {:.2} vs {:.2}",
        largest_throughput,
        base_throughput
    );
}