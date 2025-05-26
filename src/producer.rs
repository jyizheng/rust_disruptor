use std::sync::Arc;
use std::marker::PhantomData;
use crate::event::Event;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::ClaimedSequenceGuard;
use crate::sequencer::Sequencer;



pub struct Producer<T: Event> {
    sequencer: Arc<Sequencer>,
    ring_buffer: Arc<RingBuffer<T>>,
    _marker: PhantomData<T>,
}

impl<T: Event> Producer<T> {
    pub fn new(sequencer: Arc<Sequencer>, ring_buffer: Arc<RingBuffer<T>>) -> Self {
        Producer {
            sequencer,
            ring_buffer,
            _marker: PhantomData,
        }
    }

    /// Claims the next available sequence number from the `Sequencer`.
    ///
    /// # Returns
    /// A `ClaimedSequenceGuard` which encapsulates the claimed sequence.
    /// You *must* call `.publish()` on this guard after writing your event data.
    pub fn next(&self) -> ClaimedSequenceGuard { // CHANGED RETURN TYPE
        self.sequencer.next() // This now correctly calls Sequencer::next() which returns ClaimedSequenceGuard
    }

    /// Provides mutable access to the RingBuffer entry for a given sequence.
    ///
    /// Note: The specific implementation of `get_mut` will depend on how your `RingBuffer`
    /// stores and provides mutable access to its events (e.g., using `MutexGuard`).
    //pub fn get_mut(&self, sequence: i64) -> &mut T {
        // This is a placeholder. You'd use self.ring_buffer to get mutable access.
        // For example, if your RingBuffer has `data: Mutex<Vec<T>>`, it might look like:
        // let mut data_guard = self.ring_buffer.data.lock().unwrap();
        // &mut data_guard[(sequence % self.ring_buffer.capacity as i64) as usize]
    //    unimplemented!("Implement actual mutable access to RingBuffer entry")
    //}

    pub unsafe fn get_mut(&self, sequence: i64) -> &mut T { // <-- 添加 `unsafe` 关键字
        // 实现实际的 RingBuffer 条目访问逻辑
        // 由于 ring_buffer.get_mut 已经是 unsafe 函数，直接调用即可
        self.ring_buffer.get_mut(sequence) 
    }


    // The `publish` method on the `Producer` struct is NO LONGER NEEDED.
    // The responsibility for publishing now belongs to the `ClaimedSequenceGuard`
    // that `Producer::next()` returns.
    //
    // pub fn publish(&self, sequence: i64) {
    //     self.sequencer.publish(sequence); // This would cause a compile error now
    // }
}
