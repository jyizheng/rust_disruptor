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
    /// This method is `unsafe` because it provides a mutable reference
    /// to data within an `UnsafeCell`. The caller must guarantee that
    /// no other thread is concurrently writing to or reading from this
    /// specific slot. In the Disruptor model, the `Sequencer` provides
    /// this guarantee by ensuring only one producer claims a slot,
    /// and consumers only read after an event is published.
    pub fn get_mut(&self, sequence: i64) -> &mut T { // Changed from &mut self to &self
        let index = (sequence & self.index_mask) as usize;
        unsafe {
            // Dereference the UnsafeCell to get a mutable pointer, then convert to &mut T
            &mut *self.entries[index].get()
        }
    }

    /// Gets an immutable reference to an event at a given sequence number.
    pub fn get(&self, sequence: i64) -> &T {
        let index = (sequence & self.index_mask) as usize;
        unsafe {
            // Dereference the UnsafeCell to get an immutable pointer, then convert to &T
            &*self.entries[index].get()
        }
    }

    pub fn capacity(&self) -> i64 {
        self.capacity
    }
}

unsafe impl<T: Event> Sync for RingBuffer<T> {}

