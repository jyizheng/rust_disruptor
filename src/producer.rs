// src/producer.rs

use std::sync::Arc;
use std::marker::PhantomData;

use crate::event::{Event}; 
use crate::ring_buffer::RingBuffer; 
use crate::sequencer::{Sequencer, ClaimedSequenceGuard}; 

pub struct Producer<T: Event> {
    pub sequencer: Arc<Sequencer>,
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

    pub fn next(&self) -> ClaimedSequenceGuard {
        self.sequencer.next() 
    }

    pub unsafe fn get_mut(&self, sequence: i64) -> &mut T { 
        self.ring_buffer.get_mut(sequence) 
    }
}

