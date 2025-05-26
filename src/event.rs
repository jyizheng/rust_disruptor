// src/event.rs

// --- Define Event Trait ---
// Any type that can serve as an event in the ring buffer must implement this trait.
pub trait Event: Sized + Send + Sync + 'static + Default + Copy + Clone + std::fmt::Debug {}

// --- MyEvent Struct ---
// A simple example event, used to store data in the ring buffer.
#[derive(Debug, Clone, Copy, Default)]
pub struct MyEvent {
    pub value: u64,
}

// Implement the Event trait for MyEvent.
impl Event for MyEvent {}