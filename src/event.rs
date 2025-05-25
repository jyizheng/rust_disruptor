// src/event.rs

pub trait Event: Default + Clone + Send + 'static {}

// Example concrete event type
#[derive(Default, Clone, Debug)]
pub struct MyEvent {
    pub value: usize,
}

impl Event for MyEvent {}

