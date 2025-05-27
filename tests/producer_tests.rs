// tests/producer_tests.rs

use rust_disruptor::event::MyEvent;
use rust_disruptor::producer::Producer;
use rust_disruptor::ring_buffer::RingBuffer;
use rust_disruptor::sequencer::{ProducerMode, Sequencer};
use rust_disruptor::wait_strategy::BusySpinWaitStrategy; // Using a real simple strategy
use std::sync::Arc;

fn setup_producer(
    buffer_size: usize,
    producer_mode: ProducerMode,
) -> (Producer<MyEvent>, Arc<RingBuffer<MyEvent>>, Arc<Sequencer>) {
    let ring_buffer = Arc::new(RingBuffer::<MyEvent>::new(buffer_size));
    let wait_strategy = BusySpinWaitStrategy::default(); // Or your mock
    let sequencer = Arc::new(Sequencer::new(
        buffer_size,
        Arc::new(wait_strategy), // Sequencer expects Arc<dyn WaitStrategy>
        producer_mode,
    ));
    let producer = Producer::new(Arc::clone(&sequencer), Arc::clone(&ring_buffer));
    (producer, ring_buffer, sequencer)
}

#[test]
fn test_producer_new() {
    let buffer_size = 8;
    let (producer, ring_buffer_orig, sequencer_orig) =
        setup_producer(buffer_size, ProducerMode::Single);

    // Check if the Arcs point to the same objects.
    // This is a bit indirect. A more direct way would be to check raw pointers if truly needed,
    // but typically, we trust Arc::clone and the constructor.
    // For a unit test, simply ensuring it compiles and doesn't panic is often the first step.
    assert!(Arc::ptr_eq(&producer.sequencer, &sequencer_orig));
    // producer.ring_buffer is not public, so we can't directly compare.
    // The fact that get_mut works later implies it was set up.
    // We are testing the Producer::new constructor
}

#[test]
fn test_producer_next_and_get_mut_single_event() {
    let buffer_size = 8;
    let (producer, ring_buffer, _sequencer) = setup_producer(buffer_size, ProducerMode::Single);

    // 1. Claim a slot using producer.next()
    let guard = producer.next(); //
    let sequence = guard.sequence();

    assert_eq!(sequence, 0, "First claimed sequence should be 0");

    // 2. Get mutable access to the event in that slot
    let event_value_to_write = 42u64;
    unsafe {
        let event = producer.get_mut(sequence); //
        event.value = event_value_to_write;
    }

    // 3. (Optional for this specific unit test, but good practice) Publish it
    // guard.publish(); // This would make it visible via sequencer.get_highest_available_sequence

    // 4. Verify directly on the ring_buffer (since we have access in the test)
    // This confirms producer.get_mut() indeed gave a reference to the correct slot.
    unsafe {
        let event_in_buffer = ring_buffer.get(sequence);
        assert_eq!(
            event_in_buffer.value, event_value_to_write,
            "Value written through producer should be readable directly from ring buffer"
        );
    }
}

#[test]
fn test_producer_publish_updates_sequencer() {
    let buffer_size = 8;
    let (producer, _ring_buffer, sequencer) = setup_producer(buffer_size, ProducerMode::Single);

    let initial_highest_available = sequencer.get_highest_available_sequence(-1);
    assert_eq!(initial_highest_available, -1, "Initially, no sequences should be available");

    // Claim, write, and publish
    let guard = producer.next();
    let sequence = guard.sequence(); // Should be 0
    unsafe {
        let event = producer.get_mut(sequence);
        event.value = 123;
    }
    guard.publish(); // This calls sequencer.mark_sequence_as_published()

    let after_publish_highest_available = sequencer.get_highest_available_sequence(-1);
    assert_eq!(
        after_publish_highest_available, sequence,
        "After publishing, sequence should be available"
    );
    assert_eq!(sequencer.producer_cursor.get(), sequence, "Producer cursor should be updated to the published sequence");
}

#[test]
fn test_producer_multiple_claims_and_publishes() {
    let buffer_size = 8;
    let (producer, ring_buffer, sequencer) = setup_producer(buffer_size, ProducerMode::Single);

    let values_to_write = [10, 20, 30];

    for (i, &value) in values_to_write.iter().enumerate() {
        let expected_sequence = i as i64;
        let guard = producer.next();
        let sequence = guard.sequence();
        assert_eq!(sequence, expected_sequence);

        unsafe {
            let event = producer.get_mut(sequence);
            event.value = value;
        }
        guard.publish(); // Publish immediately

        // Check sequencer state
        assert_eq!(sequencer.producer_cursor.get(), expected_sequence);
        assert_eq!(sequencer.get_highest_available_sequence(expected_sequence - 1), expected_sequence);

        // Check ring buffer content
        unsafe {
            assert_eq!(ring_buffer.get(sequence).value, value);
        }
    }
}

// Test for ProducerMode::Multi behavior (mainly if Sequencer behaves differently, producer itself doesn't change much)
#[test]
fn test_producer_with_multi_producer_mode_sequencer() {
    let buffer_size = 8;
    // Sequencer is set to Multi, Producer itself doesn't have a mode.
    let (producer, _ring_buffer, sequencer) = setup_producer(buffer_size, ProducerMode::Multi);

    let guard = producer.next();
    let sequence = guard.sequence();
    assert_eq!(sequence, 0);
    guard.publish();

    assert_eq!(sequencer.producer_cursor.get(), 0);
    assert_eq!(sequencer.get_highest_available_sequence(-1), 0);

    // The producer's API usage is the same. The difference is in how the
    // sequencer handles claim_sequence internally for Multi vs Single.
    // So this test mostly ensures the producer works fine when backed by a Multi mode sequencer.
}

