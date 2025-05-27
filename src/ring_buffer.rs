use crate::event::{Event};
use std::cell::UnsafeCell; // New import!

pub struct RingBuffer<T: Event> {
    // Wrap entries in UnsafeCell to allow mutable access through &self reference
    entries: Box<[UnsafeCell<T>]>, // Changed from Vec<T> to Box<[UnsafeCell<T>]>
    capacity: i64,
    index_mask: i64,
}


impl<T: Event> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Ring buffer capacity must be greater than 0");
        assert!(capacity.is_power_of_two(), "Ring buffer capacity must be a power of two");

        // Create a vector of UnsafeCell<T>, initialized with default values
        let mut entries_vec: Vec<UnsafeCell<T>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            entries_vec.push(UnsafeCell::new(T::default()));
        }

        RingBuffer {
            // Convert Vec to Box<[T]> for fixed-size allocation on heap
            entries: entries_vec.into_boxed_slice(),
            capacity: capacity as i64,
            index_mask: (capacity - 1) as i64,
        }
    }

    /// Gets a mutable reference to an event at a given sequence number.
    ///
    /// # Safety
    /// The caller must ensure that no other thread is concurrently writing to or reading
    /// from this specific slot, and that this slot has been properly claimed for writing
    /// via the Sequencer. This typically means the sequence number was obtained from a
    /// ClaimedSequenceGuard.
    #[allow(clippy::mut_from_ref)] // Allow lint for this specific, intended pattern
    pub unsafe fn get_mut(&self, sequence: i64) -> &mut T {
        let index = (sequence & self.index_mask) as usize;
        // Safety: This is unsafe because we're bypassing Rust's borrow checker.
        // The UnsafeCell allows us to get a raw pointer, which we then dereference
        // to a mutable reference. The caller (Producer) ensures this is safe
        // by having exclusive claim to this sequence slot via the Sequencer.
        &mut *self.entries[index].get()
    }

    /// Gets an immutable reference to an event at a given sequence number.
    /// # Safety
    /// The caller must ensure that the sequence slot being read has been published
    /// and is not concurrently being written to by a producer *before* it's published.
    /// Once published, it should be safe for multiple consumers to read if no producer
    /// is wrapping around to write to this slot again.
    #[allow(clippy::mut_from_ref)] // Also allow here if get() also uses UnsafeCell similarly
                                   // (though get returning &T from &self is usually fine even with UnsafeCell if done right)
                                   // Or, more precisely for get(), it's not `mut_from_ref` but just needs safety docs if unsafe.
                                   // If get() is safe, it doesn't need this allow.
                                   // Assuming your get() also uses `&*self.entries[index].get()`:
    pub unsafe fn get(&self, sequence: i64) -> &T {
        let index = (sequence & self.index_mask) as usize;
        &*self.entries[index].get()
    }

    pub fn capacity(&self) -> i64 {
        self.capacity
    }
}

unsafe impl<T: Event> Sync for RingBuffer<T> {}

