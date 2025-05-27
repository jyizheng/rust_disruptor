// tests/sequencer_tests.rs

use rust_disruptor::sequencer::{ProducerMode, Sequence, Sequencer};
use rust_disruptor::wait_strategy::WaitStrategy;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// --- Mock WaitStrategy ---
#[derive(Clone)]
struct MockWaitStrategy {
    signal_all_calls: Arc<AtomicUsize>,
    // You can add more fields here to control behavior or record calls
    // For example, what wait_for should return:
    // wait_for_return_value: Arc<AtomicI64>,
}

impl Default for MockWaitStrategy {
    fn default() -> Self {
        MockWaitStrategy {
            signal_all_calls: Arc::new(AtomicUsize::new(0)),
            // wait_for_return_value: Arc::new(AtomicI64::new(-1)),
        }
    }
}

impl WaitStrategy for MockWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        _sequencer: Arc<Sequencer>,
        _gating_sequences: &[Arc<Sequence>],
        _consumer_sequence: Arc<Sequence>,
    ) -> i64 {
        // In a real mock, you might want to control this return value
        // For now, let's assume it allows progress or returns what's asked for if appropriate
        // return self.wait_for_return_value.load(Ordering::SeqCst);
        // For many sequencer tests, we might not even hit complex wait_for logic if gates are far ahead.
        // Or, we might want it to simply indicate that `sequence - 1` is available if we are not testing blocking.
        // A simple, often non-blocking behavior for unit tests:
        sequence - 1 // This would mean no new events are "found" by wait_for by default
    }

    fn signal_all(&self) {
        self.signal_all_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn clone_box(&self) -> Box<dyn WaitStrategy> {
        Box::new(self.clone())
    }
}
// --- End Mock WaitStrategy ---

#[test]
fn test_sequencer_initialization_defaults() {
    let buffer_size: usize = 16; // Must be power of two
    let mock_wait_strategy = Arc::new(MockWaitStrategy::default());
    let sequencer = Sequencer::new(
        buffer_size,
        mock_wait_strategy as Arc<dyn WaitStrategy>, // Cast to dyn WaitStrategy
        ProducerMode::Single,
    );

    assert_eq!(sequencer.producer_cursor.get(), -1, "Producer cursor should initialize to -1"); //
    assert_eq!(sequencer.buffer_size, buffer_size as i64, "Buffer size mismatch"); //
    // index_mask = buffer_size - 1
    // index_shift = log2(buffer_size)
    // These fields are not public, so direct testing is harder.
    // We can test them indirectly via behavior of `mark_sequence_as_published` or `get_highest_available_sequence`
    // or by making them pub(crate) for testing if absolutely necessary.

    // Test available_buffer initialization (indirectly, or if it were inspectable)
    // For now, we assume it's initialized to -1s as per implementation.
    // let initial_consumer_sequence = -1;
    // assert_eq!(sequencer.get_highest_available_sequence(initial_consumer_sequence), -1, "Highest available should be -1 initially");
}

#[test]
#[should_panic(expected = "Buffer size must be greater than 0")]
fn test_sequencer_new_panics_on_zero_buffer_size() {
    let mock_wait_strategy = Arc::new(MockWaitStrategy::default());
    Sequencer::new(0, mock_wait_strategy, ProducerMode::Single); //
}

#[test]
#[should_panic(expected = "Buffer size must be a power of two")]
fn test_sequencer_new_panics_on_non_power_of_two_buffer_size() {
    let mock_wait_strategy = Arc::new(MockWaitStrategy::default());
    Sequencer::new(10, mock_wait_strategy, ProducerMode::Single); //
}

#[test]
fn test_sequence_struct_operations() {
    let seq = Sequence::new(5);
    assert_eq!(seq.get(), 5); //

    seq.set(10); //
    assert_eq!(seq.get(), 10);

    let prev = seq.fetch_add(3); //
    assert_eq!(prev, 10);
    assert_eq!(seq.get(), 13);
}


#[test]
fn test_add_and_get_minimum_gating_sequence() {
    let buffer_size: usize = 16;
    let mock_wait_strategy = Arc::new(MockWaitStrategy::default());
    let sequencer = Sequencer::new(
        buffer_size,
        mock_wait_strategy,
        ProducerMode::Single, // Mode affects single_consumer_gate_cache
    );

    // 1. No gating sequences
    assert_eq!(sequencer.get_minimum_gating_sequence(), -1, "Min gating should be -1 with no gates"); //

    // 2. Add one gating sequence
    let gate1 = Arc::new(Sequence::new(5));
    sequencer.add_gating_sequence(Arc::clone(&gate1)); //
    assert_eq!(sequencer.get_minimum_gating_sequence(), 5, "Min gating should be gate1's value");

    // Check SPSC cache (ProducerMode::Single)
    // This is an internal detail, but its effect is on get_minimum_gating_sequence performance.
    // We can verify the cache is used by ensuring the value is correct.
    // If single_consumer_gate_cache was public, we could inspect it.
    // For now, just ensuring the correct value is returned implies it works.

    // 3. Add another gating sequence
    let gate2 = Arc::new(Sequence::new(2));
    sequencer.add_gating_sequence(Arc::clone(&gate2));
    assert_eq!(sequencer.get_minimum_gating_sequence(), 2, "Min gating should be the minimum of gate1 and gate2");

    // 4. Change sequence values
    gate1.set(1);
    assert_eq!(sequencer.get_minimum_gating_sequence(), 1, "Min gating should update to new minimum");

    gate2.set(0);
    gate1.set(-1); // even with negative values
    assert_eq!(sequencer.get_minimum_gating_sequence(), -1, "Min gating should handle negative values");
}

#[test]
fn test_single_consumer_gate_cache_logic() {
    let buffer_size: usize = 16;
    
    // Test with Single Producer Mode
    let mock_wait_strategy_single = Arc::new(MockWaitStrategy::default());
    let sequencer_single = Sequencer::new(
        buffer_size,
        mock_wait_strategy_single,
        ProducerMode::Single,
    );
    let gate_s1 = Arc::new(Sequence::new(10));
    sequencer_single.add_gating_sequence(Arc::clone(&gate_s1));
    assert_eq!(sequencer_single.get_minimum_gating_sequence(), 10, "SPSC cache should return gate_s1 value");
    gate_s1.set(5);
     assert_eq!(sequencer_single.get_minimum_gating_sequence(), 5, "SPSC cache should reflect updated gate_s1 value");

    let gate_s2 = Arc::new(Sequence::new(0));
    sequencer_single.add_gating_sequence(Arc::clone(&gate_s2)); // Adding a second gate
    assert_eq!(sequencer_single.get_minimum_gating_sequence(), 0, "SPSC cache should be invalidated, return min of gate_s1, gate_s2");


    // Test with Multi Producer Mode (cache should not be active)
    let mock_wait_strategy_multi = Arc::new(MockWaitStrategy::default());
    let sequencer_multi = Sequencer::new(
        buffer_size,
        mock_wait_strategy_multi,
        ProducerMode::Multi,
    );
    let gate_m1 = Arc::new(Sequence::new(20));
    sequencer_multi.add_gating_sequence(Arc::clone(&gate_m1));
    assert_eq!(sequencer_multi.get_minimum_gating_sequence(), 20, "MultiProducer mode should not use SPSC cache, return gate_m1 value");
    
    let gate_m2 = Arc::new(Sequence::new(15));
    sequencer_multi.add_gating_sequence(Arc::clone(&gate_m2));
    assert_eq!(sequencer_multi.get_minimum_gating_sequence(), 15, "MultiProducer mode, return min of gate_m1, gate_m2");
}

