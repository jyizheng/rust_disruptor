// tests/disruptor_tests.rs

use rust_disruptor::disruptor::Disruptor;
use rust_disruptor::event::MyEvent;
use rust_disruptor::sequencer::{ProducerMode, Sequence, Sequencer};
use rust_disruptor::wait_strategy::{BusySpinWaitStrategy, WaitStrategy};
use std::sync::Arc;

#[test]
fn test_disruptor_new() {
    let buffer_size = 32;
    let wait_strategy = BusySpinWaitStrategy::default();
    let producer_mode = ProducerMode::Single;

    let disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        buffer_size,
        wait_strategy.clone(),
        producer_mode,
    );

    assert_eq!(disruptor.producer_mode(), producer_mode);
}

#[test]
fn test_disruptor_create_producer_single_mode_first_succeeds() {
    let buffer_size = 8;
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        buffer_size,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );

    // First producer creation should succeed
    let producer1 = disruptor.create_producer();
    let sequencer_ref = Arc::clone(&producer1.sequencer);
    assert_eq!(sequencer_ref.buffer_size, buffer_size as i64);
    // You could add more assertions about producer1 if needed
}

#[test]
#[should_panic(expected = "Cannot create more than one producer in SingleProducer mode.")]
fn test_disruptor_create_producer_single_mode_second_panics() {
    let buffer_size = 8;
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        buffer_size,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );

    disruptor.create_producer(); // Create the first one successfully
    disruptor.create_producer(); // This second call should panic
}

#[test]
fn test_disruptor_create_producer_multi_mode() {
    let buffer_size = 8;
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        buffer_size,
        BusySpinWaitStrategy::default(),
        ProducerMode::Multi,
    );

    let producer1 = disruptor.create_producer();
    let producer2 = disruptor.create_producer();

    assert!(Arc::ptr_eq(&producer1.sequencer, &producer2.sequencer));
    assert_eq!(producer1.sequencer.buffer_size, buffer_size as i64);
}

#[test]
fn test_disruptor_create_consumer_wiring_via_producer_sequencer_ref() {
    let buffer_size = 16;
    let mut disruptor = Disruptor::<MyEvent, BusySpinWaitStrategy>::new(
        buffer_size,
        BusySpinWaitStrategy::default(),
        ProducerMode::Single,
    );

    let producer = disruptor.create_producer();
    let sequencer_ref_from_producer = Arc::clone(&producer.sequencer);

    let initial_min_gate = sequencer_ref_from_producer.get_minimum_gating_sequence();
    assert_eq!(initial_min_gate, -1, "Initial min gate should be -1 (no consumers)");

    let consumer1 = disruptor.create_consumer(vec![]);
    assert_eq!(consumer1.sequence.get(), -1, "New consumer sequence should be -1");
    assert!(Arc::ptr_eq(&consumer1.sequencer, &sequencer_ref_from_producer));

    consumer1.sequence.set(5);
    let min_gate_after_c1 = sequencer_ref_from_producer.get_minimum_gating_sequence();
    assert_eq!(min_gate_after_c1, 5, "Sequencer should be gated by consumer1's sequence");

    let consumer2_deps = vec![Arc::clone(&consumer1.sequence)];
    let consumer2 = disruptor.create_consumer(consumer2_deps.clone());
    assert_eq!(consumer2.sequence.get(), -1);
    assert!(Arc::ptr_eq(&consumer2.sequencer, &sequencer_ref_from_producer));

    consumer2.sequence.set(2);
    let min_gate_after_c2 = sequencer_ref_from_producer.get_minimum_gating_sequence();
    assert_eq!(min_gate_after_c2, 2, "Sequencer min gate should be consumer2's sequence");

    assert_eq!(consumer2.gating_sequences_for_wait.len(), 1 + consumer2_deps.len());
    assert!(consumer2.gating_sequences_for_wait.iter().any(|s| Arc::ptr_eq(s, &sequencer_ref_from_producer.producer_cursor)));
    assert!(consumer2.gating_sequences_for_wait.iter().any(|s| Arc::ptr_eq(s, &consumer1.sequence)));
}