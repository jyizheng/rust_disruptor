// src/producer.rs

use crate::event::Event;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::Sequencer;
use std::marker::PhantomData;
use std::sync::Arc;

/// The `Producer` is responsible for writing events to the `RingBuffer`.
///
/// It interacts with the `Sequencer` to claim the next available slot
/// and with the `RingBuffer` to write the actual event data.
pub struct Producer<T: Event> {
    sequencer: Arc<Sequencer>,
    ring_buffer: Arc<RingBuffer<T>>,
    _marker: PhantomData<T>, // Marker to tie T to the struct
}

impl<T: Event> Producer<T> {
    /// Creates a new `Producer`.
    ///
    /// # Arguments
    /// * `sequencer`: An `Arc` to the shared `Sequencer`.
    /// * `ring_buffer`: An `Arc` to the shared `RingBuffer`.
    pub fn new(sequencer: Arc<Sequencer>, ring_buffer: Arc<RingBuffer<T>>) -> Self {
        Producer {
            sequencer,
            ring_buffer,
            _marker: PhantomData,
        }
    }

    /// Claims the next available sequence number from the `Sequencer`.
    ///
    /// This method is typically called before writing an event.
    /// It ensures that the producer gets an exclusive slot to write into.
    pub fn next(&self) -> i64 {
        self.sequencer.next()
    }

    /// Gets a mutable reference to the event at the given sequence number.
    ///
    /// After claiming a sequence, the producer uses this to access the
    /// corresponding slot in the ring buffer and populate it with data.
    pub fn get_mut(&mut self, sequence: i64) -> &mut T {
        // We can safely call `get_mut` here because the `sequencer.next()` call
        // (in a real Disruptor) would ensure this slot is available for us.
        // Also, it's safe because `RingBuffer` itself is not `Sync` on `get_mut`,
        // meaning only one thread can have a mutable reference at a time.
        // However, the `Arc` around it allows shared ownership of the underlying
        // data. In a multi-producer scenario, `RingBuffer`'s `get_mut` would need
        // further atomic guarantees or careful design. For our single producer, this is fine.
        unsafe {
            // This unsafe block is due to the `Arc<RingBuffer>` and wanting a mutable
            // reference. A safer design for multi-producer would pass a mutable reference
            // of the RingBuffer directly to the producer, or use interior mutability
            // (e.g., `Mutex` or `RwLock` for each slot, which defeats performance)
            // or `UnsafeCell` with careful atomics for slot access.
            // For a single producer, this is "logically safe" but requires `unsafe`.
            // Let's refine this to be safer in a real implementation.
            // A more idiomatic safe Rust approach for single producer is to have the Producer
            // own the RingBuffer, or receive a mutable reference directly.
            // However, to keep it consistent with the shared `Arc` design for sequencer/consumers:
            // A common Rust way would be to wrap `RingBuffer` in a `Mutex` or `RwLock` if it needs
            // to be shared mutably, but this adds overhead.
            // For *this* single-producer model, where the producer claims a unique sequence,
            // the `RingBuffer`'s internal data can be accessed safely. The `unsafe` is because
            // `Arc` generally doesn't provide `&mut` access.

            // Let's remove the `unsafe` and adjust the `get_mut` access.
            // A producer *claims* a sequence, then writes to its owned slot.
            // The `RingBuffer` is accessed through its methods, but the `Arc`
            // prevents direct `&mut` unless we use `Arc::get_mut`, which requires
            // exclusive ownership (only one Arc, no clones), which isn't our case.

            // To avoid `unsafe` here and keep the `Arc` shared model, we'd typically
            // pass a mutable reference *into* a method that processes the claimed slot,
            // rather than the producer holding a mutable ref to the whole buffer.
            // Or, we'd need to rethink the `RingBuffer`'s internal mutability.

            // Let's go with a simpler model for now: the `Producer` doesn't hold `&mut`
            // to the whole buffer. Instead, it interacts via methods.
            // For `get_mut`, the `RingBuffer` needs to expose its internal `entries` mutably
            // in a way that `Arc` allows.
            // This often means `RingBuffer` itself has an `UnsafeCell` or `Mutex` internally.
            // For LMAX style, `UnsafeCell` + atomics is more likely.

            // Let's simplify this. The `RingBuffer` *is* inherently mutable by the producer.
            // The `Producer` needs to modify it. `Arc` implies shared immutable access,
            // unless we wrap the inner content in a `Mutex` or `RwLock`.
            // For a Disruptor, `Mutex`/`RwLock` on the *entire* RingBuffer is too slow.
            // Individual slots could be `UnsafeCell<T>`.

            // For now, let's assume the producer has exclusive write access to the slot it claimed
            // and use an `UnsafeCell` inside the `RingBuffer` to allow mutable access through `&self`.
            // This is a common pattern in high-performance Rust when mimicking C/C++ memory models.

            // *Self-correction*: The previous `RingBuffer` definition with `Vec<T>`
            // requires `&mut self` for `get_mut`. An `Arc<RingBuffer>` implies shared immutable
            // access to `RingBuffer` struct itself. To get mutable interior access to `entries`,
            // `entries` itself needs to be wrapped in an `UnsafeCell` or similar.
            //
            // Let's modify `RingBuffer` to allow interior mutability using `UnsafeCell`.
            // This is necessary to uphold the single-producer-writes-to-claimed-slot
            // semantics without `Arc::get_mut()` which isn't applicable for shared `Arc`s.
        }
        // *Corrected access* after modifying RingBuffer
        self.ring_buffer.get_mut(sequence)
    }

    /// Publishes the given sequence number, making the event visible to consumers.
    ///
    /// After writing data into the slot obtained from `get_mut`, the producer
    /// calls this to commit the event.
    pub fn publish(&self, sequence: i64) {
        self.sequencer.publish(sequence);
    }
}


