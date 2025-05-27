// tests/wait_strategy_tests.rs

use rust_disruptor::sequencer::{ProducerMode, Sequence, Sequencer};
use rust_disruptor::wait_strategy::*; // Import all strategies and the trait
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

// Helper function to set up a sequencer and related sequences for tests
fn setup_sequencer_and_sequences(
    buffer_size: usize,
) -> (Arc<Sequencer>, Arc<Sequence>, Vec<Arc<Sequence>>) {
    #[derive(Clone, Default)]
    struct DummyWait;
    impl WaitStrategy for DummyWait {
        fn wait_for(&self, s: i64, _: Arc<Sequencer>, _: &[Arc<Sequence>], _: Arc<Sequence>) -> i64 {
            s - 1
        }
        fn signal_all(&self) {}
        fn clone_box(&self) -> Box<dyn WaitStrategy> {
            Box::new(self.clone())
        }
    }

    let sequencer = Arc::new(Sequencer::new(
        buffer_size,
        Arc::new(DummyWait::default()) as Arc<dyn WaitStrategy>,
        ProducerMode::Single,
    ));
    let consumer_sequence = Arc::new(Sequence::new(-1));
    let gating_sequences_for_test = vec![Arc::clone(&sequencer.producer_cursor)];
    (sequencer, consumer_sequence, gating_sequences_for_test)
}

// --- Tests for BusySpinWaitStrategy ---
#[test]
fn test_busy_spin_wait_strategy_returns_when_sequence_available() {
    let strategy = BusySpinWaitStrategy::default();
    let (sequencer, consumer_sequence, gating_sequences) = setup_sequencer_and_sequences(16);
    let desired_sequence = 0i64;
    sequencer.producer_cursor.set(desired_sequence);
    sequencer.mark_sequence_as_published(desired_sequence); //
    let available = strategy.wait_for(
        desired_sequence,
        Arc::clone(&sequencer),
        &gating_sequences,
        Arc::clone(&consumer_sequence),
    );
    assert_eq!(
        available, desired_sequence,
        "Should return desired sequence if available"
    );
}

#[test]
fn test_busy_spin_wait_strategy_spins_and_returns_on_progress() {
    let strategy = BusySpinWaitStrategy::default();
    let (sequencer_main_thread, consumer_sequence, gating_sequences) =
        setup_sequencer_and_sequences(16);
    let desired_sequence = 5i64;
    consumer_sequence.set(desired_sequence - 1);
    let processing_done = Arc::new(AtomicBool::new(false));
    let processing_done_clone_for_consumer = Arc::clone(&processing_done); // Clone for consumer
    let sequencer_for_thread = Arc::clone(&sequencer_main_thread);
    let consumer_sequence_for_thread = Arc::clone(&consumer_sequence);
    let gating_sequences_for_thread = gating_sequences.iter().map(Arc::clone).collect::<Vec<_>>();
    let handle = thread::spawn(move || {
        let available = strategy.wait_for(
            desired_sequence,
            sequencer_for_thread,
            &gating_sequences_for_thread,
            consumer_sequence_for_thread,
        );
        assert_eq!(available, desired_sequence);
        processing_done_clone_for_consumer.store(true, Ordering::SeqCst); // Use consumer's clone
    });
    thread::sleep(Duration::from_millis(10));
    assert!(
        !processing_done.load(Ordering::SeqCst), // Main thread checks original Arc
        "Consumer should be spinning"
    );
    sequencer_main_thread
        .producer_cursor
        .set(desired_sequence);
    sequencer_main_thread.mark_sequence_as_published(desired_sequence); //
    handle.join().expect("Consumer thread panicked");
    assert!(
        processing_done.load(Ordering::SeqCst), // Main thread checks original Arc
        "Consumer thread should have completed"
    );
}

// --- Tests for YieldingWaitStrategy ---
#[test]
fn test_yielding_wait_strategy_returns_when_sequence_available() {
    let strategy = YieldingWaitStrategy::default();
    let (sequencer, consumer_sequence, gating_sequences) = setup_sequencer_and_sequences(16);
    let desired_sequence = 0i64;
    sequencer.producer_cursor.set(desired_sequence);
    sequencer.mark_sequence_as_published(desired_sequence); //
    let available = strategy.wait_for(
        desired_sequence,
        Arc::clone(&sequencer),
        &gating_sequences,
        Arc::clone(&consumer_sequence),
    );
    assert_eq!(available, desired_sequence);
}

#[test]
fn test_yielding_wait_strategy_yields_and_returns_on_progress() {
    let strategy = YieldingWaitStrategy::default();
    let (sequencer_main_thread, consumer_sequence, gating_sequences) =
        setup_sequencer_and_sequences(16);
    let desired_sequence = 3i64;
    consumer_sequence.set(desired_sequence - 1);
    let processing_done = Arc::new(AtomicBool::new(false));
    let processing_done_clone_for_consumer = Arc::clone(&processing_done); // Clone for consumer
    let sequencer_for_thread = Arc::clone(&sequencer_main_thread);
    let consumer_sequence_for_thread = Arc::clone(&consumer_sequence);
    let gating_sequences_for_thread = gating_sequences.iter().map(Arc::clone).collect::<Vec<_>>();
    let handle = thread::spawn(move || {
        let available = strategy.wait_for(
            desired_sequence,
            sequencer_for_thread,
            &gating_sequences_for_thread,
            consumer_sequence_for_thread,
        );
        assert_eq!(available, desired_sequence);
        processing_done_clone_for_consumer.store(true, Ordering::SeqCst); // Use consumer's clone
    });
    thread::sleep(Duration::from_millis(20));
    assert!(
        !processing_done.load(Ordering::SeqCst), // Main thread checks original Arc
        "Consumer should be yielding"
    );
    sequencer_main_thread
        .producer_cursor
        .set(desired_sequence);
    sequencer_main_thread.mark_sequence_as_published(desired_sequence); //
    handle.join().expect("Consumer thread panicked");
    assert!(processing_done.load(Ordering::SeqCst)); // Main thread checks original Arc
}

// --- Tests for BlockingWaitStrategy ---
#[test]
fn test_blocking_wait_strategy_returns_when_sequence_available_no_block() {
    let strategy = BlockingWaitStrategy::default();
    let (sequencer, consumer_sequence, gating_sequences) = setup_sequencer_and_sequences(16);
    let desired_sequence = 0i64;
    sequencer.producer_cursor.set(desired_sequence);
    sequencer.mark_sequence_as_published(desired_sequence); //
    let available = strategy.wait_for(
        desired_sequence,
        Arc::clone(&sequencer),
        &gating_sequences,
        Arc::clone(&consumer_sequence),
    );
    assert_eq!(available, desired_sequence);
}

#[test]
fn test_blocking_wait_strategy_blocks_and_wakes_up() {
    let strategy = Arc::new(BlockingWaitStrategy::default());
    let (sequencer, consumer_sequence, gating_sequences) = setup_sequencer_and_sequences(16);
    let desired_sequence = 1i64;
    consumer_sequence.set(desired_sequence - 1);

    let barrier = Arc::new(Barrier::new(2));
    let consumer_finished_processing = Arc::new(AtomicBool::new(false)); // Original Arc

    let strategy_clone_consumer = Arc::clone(&strategy);
    let sequencer_clone_consumer = Arc::clone(&sequencer);
    let consumer_sequence_clone = Arc::clone(&consumer_sequence);
    let gating_sequences_clone_consumer = gating_sequences.iter().map(Arc::clone).collect::<Vec<_>>();
    let barrier_clone_consumer = Arc::clone(&barrier);
    let consumer_finished_clone_for_consumer_thread = Arc::clone(&consumer_finished_processing); // Clone for consumer thread

    let consumer_handle = thread::spawn(move || {
        barrier_clone_consumer.wait();
        let available = strategy_clone_consumer.wait_for(
            desired_sequence,
            sequencer_clone_consumer,
            &gating_sequences_clone_consumer,
            consumer_sequence_clone,
        );
        assert_eq!(available, desired_sequence);
        consumer_finished_clone_for_consumer_thread.store(true, Ordering::SeqCst);
    });

    let strategy_clone_producer = Arc::clone(&strategy);
    let sequencer_clone_producer = Arc::clone(&sequencer);
    let barrier_clone_producer = Arc::clone(&barrier);
    // --- FIX: Clone consumer_finished_processing for producer thread ---
    let consumer_finished_clone_for_producer_thread = Arc::clone(&consumer_finished_processing);

    let producer_handle = thread::spawn(move || {
        barrier_clone_producer.wait();
        thread::sleep(Duration::from_millis(50));
        assert!(
            !consumer_finished_clone_for_producer_thread.load(Ordering::SeqCst), // Use clone here
            "Consumer should be blocked"
        );
        sequencer_clone_producer
            .producer_cursor
            .set(desired_sequence);
        sequencer_clone_producer.mark_sequence_as_published(desired_sequence); //
        strategy_clone_producer.signal_all();
    });

    consumer_handle.join().expect("Consumer thread failed");
    producer_handle.join().expect("Producer thread failed");
    assert!(
        consumer_finished_processing.load(Ordering::SeqCst), // Main thread uses original Arc
        "Consumer did not finish processing after wake up"
    );
}

// --- Tests for PhasedBackoffWaitStrategy ---
#[test]
fn test_phased_backoff_returns_when_sequence_available_no_block() {
    let strategy = PhasedBackoffWaitStrategy::default();
    let (sequencer, consumer_sequence, gating_sequences) = setup_sequencer_and_sequences(16);
    let desired_sequence = 0i64;
    sequencer.producer_cursor.set(desired_sequence);
    sequencer.mark_sequence_as_published(desired_sequence); //
    let available = strategy.wait_for(
        desired_sequence,
        Arc::clone(&sequencer),
        &gating_sequences,
        Arc::clone(&consumer_sequence),
    );
    assert_eq!(available, desired_sequence);
}

#[test]
fn test_phased_backoff_strategy_blocks_and_wakes_up() {
    let strategy = Arc::new(PhasedBackoffWaitStrategy::new(
        Duration::from_micros(1),
        Duration::from_micros(1),
        BlockingWaitStrategy::default(),
    ));
    let (sequencer, consumer_sequence, gating_sequences) = setup_sequencer_and_sequences(16);
    let desired_sequence = 1i64;
    consumer_sequence.set(desired_sequence - 1);

    let barrier = Arc::new(Barrier::new(2));
    let consumer_finished_processing = Arc::new(AtomicBool::new(false)); // Original Arc

    let strategy_clone_consumer = Arc::clone(&strategy);
    let sequencer_clone_consumer = Arc::clone(&sequencer);
    let consumer_sequence_clone = Arc::clone(&consumer_sequence);
    let gating_sequences_clone_consumer = gating_sequences.iter().map(Arc::clone).collect::<Vec<_>>();
    let barrier_clone_consumer = Arc::clone(&barrier);
    let consumer_finished_clone_for_consumer_thread = Arc::clone(&consumer_finished_processing); // Clone for consumer

    let consumer_handle = thread::spawn(move || {
        barrier_clone_consumer.wait();
        let available = strategy_clone_consumer.wait_for(
            desired_sequence,
            sequencer_clone_consumer,
            &gating_sequences_clone_consumer,
            consumer_sequence_clone,
        );
        assert_eq!(available, desired_sequence);
        consumer_finished_clone_for_consumer_thread.store(true, Ordering::SeqCst);
    });

    let strategy_clone_producer = Arc::clone(&strategy);
    let sequencer_clone_producer = Arc::clone(&sequencer);
    let barrier_clone_producer = Arc::clone(&barrier);
    // --- FIX: Clone consumer_finished_processing for producer thread ---
    let consumer_finished_clone_for_producer_thread = Arc::clone(&consumer_finished_processing);

    let producer_handle = thread::spawn(move || {
        barrier_clone_producer.wait();
        thread::sleep(Duration::from_millis(50));
        assert!(
            !consumer_finished_clone_for_producer_thread.load(Ordering::SeqCst), // Use clone here
            "Consumer should be blocked via PhasedBackoff"
        );
        sequencer_clone_producer
            .producer_cursor
            .set(desired_sequence);
        sequencer_clone_producer.mark_sequence_as_published(desired_sequence); //
        strategy_clone_producer.signal_all();
    });

    consumer_handle.join().expect("Consumer thread failed");
    producer_handle.join().expect("Producer thread failed");
    assert!(consumer_finished_processing.load(Ordering::SeqCst)); // Main thread uses original Arc
}

#[test]
fn test_wait_strategy_clone_box() {
    let strategy1 = BusySpinWaitStrategy::default();
    let cloned_box1 = strategy1.clone_box();
    let (sequencer, consumer_sequence, gating_sequences) = setup_sequencer_and_sequences(16);
    sequencer.producer_cursor.set(0);
    sequencer.mark_sequence_as_published(0); //
    assert_eq!(
        cloned_box1.wait_for(
            0,
            Arc::clone(&sequencer),
            &gating_sequences,
            Arc::clone(&consumer_sequence)
        ),
        0
    );

    let strategy2 = BlockingWaitStrategy::default();
    let cloned_box2 = strategy2.clone_box();
    assert_eq!(
        cloned_box2.wait_for(
            0,
            Arc::clone(&sequencer),
            &gating_sequences,
            Arc::clone(&consumer_sequence)
        ),
        0
    );

    let strategy3 = YieldingWaitStrategy::default();
    let cloned_box3 = strategy3.clone_box();
    assert_eq!(
        cloned_box3.wait_for(
            0,
            Arc::clone(&sequencer),
            &gating_sequences,
            Arc::clone(&consumer_sequence)
        ),
        0
    );

    let strategy4 = PhasedBackoffWaitStrategy::default();
    let cloned_box4 = strategy4.clone_box();
    assert_eq!(
        cloned_box4.wait_for(
            0,
            Arc::clone(&sequencer),
            &gating_sequences,
            Arc::clone(&consumer_sequence)
        ),
        0
    );
}
