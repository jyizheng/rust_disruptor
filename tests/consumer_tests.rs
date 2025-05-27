// tests/consumer_tests.rs

use rust_disruptor::consumer::Consumer;
use rust_disruptor::event::{Event, MyEvent};
use rust_disruptor::ring_buffer::RingBuffer;
use rust_disruptor::sequencer::{ProducerMode, Sequence, Sequencer};
use rust_disruptor::wait_strategy::WaitStrategy; // Import the trait
use std::sync::{Arc, Mutex}; // Mutex for mock state
use std::vec; // Ensure vec is imported if used explicitly (though often not needed)

// --- Mock WaitStrategy for Consumer Tests ---
#[derive(Clone)]
struct MockConsumerWaitStrategy {
    // Value that wait_for will return. Represents the highest_available_sequence.
    available_sequence_to_return: Arc<Mutex<i64>>,
    // Optionally, record calls to wait_for for more detailed assertions
    // last_waited_for_sequence: Arc<Mutex<Option<i64>>>,
}

impl MockConsumerWaitStrategy {
    fn new(initial_return_val: i64) -> Self {
        Self {
            available_sequence_to_return: Arc::new(Mutex::new(initial_return_val)),
            // last_waited_for_sequence: Arc::new(Mutex::new(None)),
        }
    }

    fn set_available_sequence(&self, seq: i64) {
        let mut guard = self.available_sequence_to_return.lock().unwrap();
        *guard = seq;
    }
}

impl Default for MockConsumerWaitStrategy {
    fn default() -> Self {
        Self::new(-1) // Default to no sequences available
    }
}

impl WaitStrategy for MockConsumerWaitStrategy {
    fn wait_for(
        &self,
        _sequence_to_claim: i64, // The sequence the consumer wants
        _sequencer_ref: Arc<Sequencer>,
        _gating_sequences_ref: &[Arc<Sequence>],
        _consumer_sequence_ref: Arc<Sequence>,
    ) -> i64 {
        // let mut last_waited = self.last_waited_for_sequence.lock().unwrap();
        // *last_waited = Some(sequence_to_claim);
        *self.available_sequence_to_return.lock().unwrap()
    }

    fn signal_all(&self) {
        // Not typically called by Consumer directly, but by Sequencer. No-op for this mock's role.
    }

    fn clone_box(&self) -> Box<dyn WaitStrategy> {
        Box::new(self.clone())
    }
}
// --- End Mock WaitStrategy ---

// Helper to set up Consumer with its dependencies for testing
fn setup_consumer_dependencies(
    buffer_size: usize,
    mock_wait_strategy: MockConsumerWaitStrategy,
    dependent_sequences_values: Option<Vec<i64>>, // Initial values for dependent sequences
) -> (
    Arc<Sequencer>,
    Arc<RingBuffer<MyEvent>>,
    MockConsumerWaitStrategy,
    Vec<Arc<Sequence>>,
) {
    // Dummy WaitStrategy for the Sequencer itself, not the one Consumer uses.
    #[derive(Clone, Default)]
    struct DummySequencerWait;
    impl WaitStrategy for DummySequencerWait {
        fn wait_for(&self, s: i64, _: Arc<Sequencer>, _: &[Arc<Sequence>], _: Arc<Sequence>) -> i64 {s - 1}
        fn signal_all(&self) {}
        fn clone_box(&self) -> Box<dyn WaitStrategy> { Box::new(self.clone()) }
    }

    let sequencer = Arc::new(Sequencer::new(
        buffer_size,
        Arc::new(DummySequencerWait::default()) as Arc<dyn WaitStrategy>,
        ProducerMode::Single,
    ));
    let ring_buffer = Arc::new(RingBuffer::<MyEvent>::new(buffer_size));

    let mut dependent_arcs = Vec::new();
    if let Some(values) = dependent_sequences_values {
        for val in values {
            dependent_arcs.push(Arc::new(Sequence::new(val)));
        }
    }

    (sequencer, ring_buffer, mock_wait_strategy, dependent_arcs)
}

#[test]
fn test_consumer_new_initialization() {
    let mock_ws = MockConsumerWaitStrategy::new(-1);
    let (sequencer, ring_buffer, ws_clone, deps) =
        setup_consumer_dependencies(16, mock_ws, None);

    let consumer = Consumer::new( //
        Arc::clone(&sequencer),
        Arc::clone(&ring_buffer),
        ws_clone, // Consumer takes ownership of this specific strategy instance
        deps.clone(),
    );

    assert_eq!(consumer.sequence.get(), -1, "Consumer sequence should init to -1");
    assert!(Arc::ptr_eq(&consumer.sequencer, &sequencer));
    assert!(Arc::ptr_eq(&consumer.ring_buffer, &ring_buffer));
    // Check gating_sequences_for_wait includes producer_cursor and dependencies
    // consumer.gating_sequences_for_wait is pub
    assert_eq!(consumer.gating_sequences_for_wait.len(), 1 + deps.len());
    assert!(consumer.gating_sequences_for_wait.iter().any(|s| Arc::ptr_eq(s, &sequencer.producer_cursor)));
    for dep_arc in deps {
        assert!(consumer.gating_sequences_for_wait.iter().any(|s| Arc::ptr_eq(s, &dep_arc)));
    }
}

#[test]
fn test_consumer_new_with_dependencies() {
    let mock_ws = MockConsumerWaitStrategy::new(-1);
    let dependent_sequences_values = vec![5, 10];
    let (sequencer, _ring_buffer, ws_clone, deps) =
        setup_consumer_dependencies(16, mock_ws, Some(dependent_sequences_values));
    let ring_buffer_for_consumer = Arc::new(RingBuffer::<MyEvent>::new(16));


    let consumer = Consumer::new(
        Arc::clone(&sequencer),
        ring_buffer_for_consumer,
        ws_clone,
        deps.clone(),
    );

    assert_eq!(consumer.gating_sequences_for_wait.len(), 1 + 2, "Expected producer cursor + 2 dependencies");
    assert!(consumer.gating_sequences_for_wait.iter().any(|s| Arc::ptr_eq(s, &sequencer.producer_cursor)));
    assert!(consumer.gating_sequences_for_wait.iter().any(|s| Arc::ptr_eq(s, &deps[0])));
    assert!(consumer.gating_sequences_for_wait.iter().any(|s| Arc::ptr_eq(s, &deps[1])));
}


#[test]
fn test_process_event_no_event_available() {
    let mock_ws = MockConsumerWaitStrategy::new(-1); // wait_for returns -1 (less than next_seq 0)
    let (sequencer, ring_buffer, ws_clone, deps) =
        setup_consumer_dependencies(16, mock_ws, None);
    let consumer = Consumer::new(sequencer, ring_buffer, ws_clone, deps);

    let mut handler_called_count = 0;
    let result = consumer.process_event(|_event: &MyEvent| { //
        handler_called_count += 1;
    });

    assert!(result.is_none(), "Should return None if no event is available");
    assert_eq!(handler_called_count, 0, "Handler should not be called");
    assert_eq!(consumer.sequence.get(), -1, "Consumer sequence should not change");
}

#[test]
fn test_process_event_one_event_available() {
    let mock_ws = MockConsumerWaitStrategy::new(0); // wait_for returns 0 (next_seq is 0)
    let (sequencer, ring_buffer, ws_clone, deps) =
        setup_consumer_dependencies(16, mock_ws, None);
    let consumer = Consumer::new(Arc::clone(&sequencer), Arc::clone(&ring_buffer), ws_clone, deps);

    // Prepare event in RingBuffer
    unsafe { ring_buffer.get_mut(0).value = 123; }
    // Sequencer also needs to know about this for some wait_strategy internal checks,
    // but our mock bypasses much of that. For a real wait strategy, this would be important.
    // For this test with MockConsumerWaitStrategy, only mock_ws.set_available_sequence matters.

    let mut processed_value = 0;
    let result = consumer.process_event(|event: &MyEvent| {
        processed_value = event.value;
    });

    assert_eq!(result, Some(0), "Should return Some(0) for the processed sequence");
    assert_eq!(processed_value, 123, "Handler should process the correct event value");
    assert_eq!(consumer.sequence.get(), 0, "Consumer sequence should update to 0");
}

#[test]
fn test_process_event_batch_no_events() {
    let mock_ws = MockConsumerWaitStrategy::new(-1);
    let (sequencer, ring_buffer, ws_clone, deps) =
        setup_consumer_dependencies(16, mock_ws, None);
    let consumer = Consumer::new(sequencer, ring_buffer, ws_clone, deps);

    let mut handler_calls = Vec::new();
    let count = consumer.process_event_batch(|event: &MyEvent, seq: i64, eob: bool| { //
        handler_calls.push((event.value, seq, eob));
    });

    assert_eq!(count, 0, "Should process 0 events");
    assert!(handler_calls.is_empty(), "Handler should not be called");
    assert_eq!(consumer.sequence.get(), -1, "Consumer sequence should not change");
}

#[test]
fn test_process_event_batch_single_event() {
    let mock_ws = MockConsumerWaitStrategy::new(0); // wait_for returns 0
    let (sequencer, ring_buffer, ws_clone, deps) =
        setup_consumer_dependencies(16, mock_ws, None);
    let consumer = Consumer::new(sequencer, Arc::clone(&ring_buffer), ws_clone, deps);

    unsafe { ring_buffer.get_mut(0).value = 100; }

    let mut handler_calls = Vec::new();
    let count = consumer.process_event_batch(|event: &MyEvent, seq: i64, eob: bool| {
        handler_calls.push((event.value, seq, eob));
    });

    assert_eq!(count, 1, "Should process 1 event");
    assert_eq!(handler_calls.len(), 1);
    assert_eq!(handler_calls[0], (100, 0, true), "Event data, sequence, and eob mismatch");
    assert_eq!(consumer.sequence.get(), 0, "Consumer sequence should update to 0");
}

#[test]
fn test_process_event_batch_multiple_events() {
    let mock_ws = MockConsumerWaitStrategy::new(2); // wait_for returns 2 (events 0, 1, 2 available)
    let (_sequencer, ring_buffer, ws_clone, deps) =
        setup_consumer_dependencies(16, mock_ws, None);
    // Sequencer is not directly used by Consumer's processing loop once wait_for result is obtained
    // But it's needed for Consumer::new to setup gating_sequences_for_wait
    let sequencer_for_consumer = Arc::new(Sequencer::new(
        16, Arc::new(MockConsumerWaitStrategy::default()) as Arc<dyn WaitStrategy>, ProducerMode::Single
    ));

    let consumer = Consumer::new(sequencer_for_consumer, Arc::clone(&ring_buffer), ws_clone, deps);


    unsafe {
        ring_buffer.get_mut(0).value = 10;
        ring_buffer.get_mut(1).value = 20;
        ring_buffer.get_mut(2).value = 30;
    }

    let mut handler_calls = Vec::new();
    let count = consumer.process_event_batch(|event: &MyEvent, seq: i64, eob: bool| {
        handler_calls.push((event.value, seq, eob));
    });

    assert_eq!(count, 3, "Should process 3 events");
    assert_eq!(handler_calls.len(), 3);
    assert_eq!(handler_calls[0], (10, 0, false));
    assert_eq!(handler_calls[1], (20, 1, false));
    assert_eq!(handler_calls[2], (30, 2, true));
    assert_eq!(consumer.sequence.get(), 2, "Consumer sequence should update to 2");
}

#[test]
fn test_process_event_batch_wait_strategy_limits_batch() {
    // Consumer wants sequence 0. WaitStrategy says only up to sequence 1 is available (batch of 2: 0, 1).
    let mock_ws = MockConsumerWaitStrategy::new(1); 
    let (_sequencer, ring_buffer, ws_clone, deps) =
        setup_consumer_dependencies(16, mock_ws, None);
    let sequencer_for_consumer = Arc::new(Sequencer::new(
        16, Arc::new(MockConsumerWaitStrategy::default()) as Arc<dyn WaitStrategy>, ProducerMode::Single
    ));
    let consumer = Consumer::new(sequencer_for_consumer, Arc::clone(&ring_buffer), ws_clone, deps);

    unsafe {
        ring_buffer.get_mut(0).value = 10;
        ring_buffer.get_mut(1).value = 20;
        ring_buffer.get_mut(2).value = 30; // This one should not be processed
    }

    let mut handler_calls = Vec::new();
    let count = consumer.process_event_batch(|event: &MyEvent, seq: i64, eob: bool| {
        handler_calls.push((event.value, seq, eob));
    });

    assert_eq!(count, 2, "Should process 2 events as limited by wait_strategy");
    assert_eq!(handler_calls.len(), 2);
    assert_eq!(handler_calls[0], (10, 0, false));
    assert_eq!(handler_calls[1], (20, 1, true)); // Event at sequence 1 is end_of_batch
    assert_eq!(consumer.sequence.get(), 1, "Consumer sequence should update to 1");
}

