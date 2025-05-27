// tests/event_tests.rs

use rust_disruptor::event::{Event, MyEvent}; // Assuming MyEvent and Event are pub

// Helper to check if a type T implements Event
// This function will only compile if T implements Event.
fn _assert_event_trait<T: Event>() {}

#[test]
fn test_myevent_is_event() {
    // This test primarily ensures that MyEvent compiles with the Event trait.
    // The _assert_event_trait function helps confirm this at compile time for MyEvent.
    _assert_event_trait::<MyEvent>();
    // We can also do a runtime instantiation if needed.
    let event = MyEvent::default();
    assert_eq!(event.value, 0, "Default value should be 0"); //
}

#[test]
fn test_myevent_creation_and_modification() {
    let mut event = MyEvent { value: 42 };
    assert_eq!(event.value, 42);

    event.value = 100;
    assert_eq!(event.value, 100);

    // Test Clone
    let event_clone = event.clone(); //
    assert_eq!(event_clone.value, 100);

    // Test Copy (if applicable, MyEvent is Copy)
    let event_copy = event; //
    assert_eq!(event_copy.value, 100);

    // Test Debug format
    let debug_format = format!("{:?}", event); //
    assert_eq!(debug_format, "MyEvent { value: 100 }");
}

