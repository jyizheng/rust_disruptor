// src/event.rs

// The `Event` trait defines a set of constraints that any type implementing it must satisfy.
// These constraints ensure that the type is:
// - `Sized`: The type's size is known at compile time.
// - `Send`: The type can be safely transferred between threads.
// - `Sync`: References to the type can be safely shared between threads.
// - `'static`: The type does not contain non-static references, making it valid for the program's lifetime.
// - `Default`: The type can provide a default value using the `Default` trait.
// - `Copy`: The type supports bitwise copying, meaning it can be duplicated without explicit cloning.
// - `Clone`: The type supports deep copying using the `Clone` trait.
// - `std::fmt::Debug`: The type can be formatted using the `{:?}` formatter for debugging purposes.
pub trait Event: Sized + Send + Sync + 'static + Default + Copy + Clone + std::fmt::Debug {}

// A simple example event, used to store data in the ring buffer.
#[derive(Debug, Clone, Copy, Default)]
pub struct MyEvent {
    pub value: u64,
}

// Implement the Event trait for MyEvent.
// Let the compile to check if Event trait's
// requirement is met or not
impl Event for MyEvent {}