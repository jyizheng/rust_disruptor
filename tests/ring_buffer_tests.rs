// tests/ring_buffer_tests.rs

use rust_disruptor::event::{Event, MyEvent}; // Assuming MyEvent is pub
use rust_disruptor::ring_buffer::RingBuffer;  // Assuming RingBuffer is pub

#[test]
fn test_ring_buffer_new_and_capacity() {
    let capacity: usize = 16; // Must be power of two
    let rb = RingBuffer::<MyEvent>::new(capacity); //
    assert_eq!(rb.capacity(), capacity as i64); //
}

#[test]
#[should_panic(expected = "Ring buffer capacity must be greater than 0")]
fn test_ring_buffer_new_panics_on_zero_capacity() {
    RingBuffer::<MyEvent>::new(0); //
}

#[test]
#[should_panic(expected = "Ring buffer capacity must be a power of two")]
fn test_ring_buffer_new_panics_on_non_power_of_two_capacity() {
    RingBuffer::<MyEvent>::new(10); //
}

#[test]
fn test_ring_buffer_get_and_get_mut() {
    let capacity: usize = 4;
    let rb = RingBuffer::<MyEvent>::new(capacity);

    // Initialize some events using get_mut (simulating producer)
    // Sequence numbers for a buffer of size 4 are 0, 1, 2, 3, then wraps to 4 (index 0), 5 (index 1) etc.

    // Slot 0 (Sequence 0)
    unsafe {
        let event0 = rb.get_mut(0); //
        event0.value = 10;
    }
    // Slot 1 (Sequence 1)
    unsafe {
        let event1 = rb.get_mut(1);
        event1.value = 20;
    }

    // Verify with get (simulating consumer)
    unsafe {
        let event0_read = rb.get(0); //
        assert_eq!(event0_read.value, 10);

        let event1_read = rb.get(1);
        assert_eq!(event1_read.value, 20);
    }

    // Test wrapping: Sequence 4 should map to index 0 for capacity 4
    unsafe {
        let event4 = rb.get_mut(4); // Index (4 & (4-1)) = (4 & 3) = 0
        event4.value = 50; // Overwrites original event at sequence 0
    }
    unsafe {
        let event0_read_again = rb.get(0);
        assert_eq!(event0_read_again.value, 50, "Value at index 0 (seq 0) should be updated by seq 4");
        let event4_read = rb.get(4);
        assert_eq!(event4_read.value, 50, "Value at index 0 (seq 4) should be 50");
    }

    // Test default initialization of unwritten slots
    // Slot 2 (Sequence 2) was never written explicitly in this test after RingBuffer::new
    unsafe {
        let event2_default = rb.get(2);
        assert_eq!(event2_default.value, 0, "Unwritten slots should hold default MyEvent value"); //
    }
}

#[test]
fn test_ring_buffer_values_are_distinct_unless_overwritten() {
    let capacity: usize = 2;
    let rb = RingBuffer::<MyEvent>::new(capacity);

    unsafe {
        rb.get_mut(0).value = 1;
        rb.get_mut(1).value = 2;
    }

    unsafe {
        assert_eq!(rb.get(0).value, 1);
        assert_eq!(rb.get(1).value, 2);
    }

    // Overwrite sequence 0 (index 0) with sequence 2 (index 0)
    unsafe {
        rb.get_mut(2).value = 3; // Wraps around, 2 & (2-1) = 0
    }

    unsafe {
        assert_eq!(rb.get(0).value, 3, "Sequence 0 (index 0) should be overwritten by sequence 2");
        assert_eq!(rb.get(2).value, 3, "Sequence 2 (index 0) should reflect the new value");
        assert_eq!(rb.get(1).value, 2, "Sequence 1 (index 1) should remain unchanged");
    }
}

