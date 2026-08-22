use std::{any::Any, panic};
use sync_wrapper::SyncWrapper;


pub struct PanicPayload {
    repr: SyncWrapper<Box<dyn Any + Send + 'static>>
}

impl PanicPayload {
    
    pub fn new(panic_payload: Box<dyn Any + Send + 'static>) -> Self {
        Self { repr: SyncWrapper::new(panic_payload) }
    }

    pub fn as_str(&self) -> Option<&str> {
        panic_payload_as_str(&self.repr)
    }

    pub fn resume_unwind(self) -> ! {
        panic::resume_unwind(self.repr.into_inner())
    }

    pub fn into_inner(self) -> Box<dyn Any + Send + 'static> {
        self.repr.into_inner()
    }
}


/// Based on code from Tokio crate
///
/// Source:
/// - https://docs.rs/tokio/1.53.1/src/tokio/runtime/task/error.rs.html
/// - Copyright (c) Tokio Contributors
/// - Licensed under the MIT License
fn panic_payload_as_str(payload: &SyncWrapper<Box<dyn Any + Send>>) -> Option<&str> {
    // Panic payloads are almost always `String` (if invoked with formatting arguments)
    // or `&'static str` (if invoked with a string literal).
    //
    // Non-string panic payloads have niche use-cases,
    // so we don't really need to worry about those.
    if let Some(s) = payload.downcast_ref_sync::<String>() {
        return Some(s);
    }

    if let Some(s) = payload.downcast_ref_sync::<&'static str>() {
        return Some(s);
    }

    None
}

/// Based on code from Tokio crate
///
/// Source:
/// - https://docs.rs/tokio/1.53.1/src/tokio/util/sync_wrapper.rs.html
/// - Copyright (c) Tokio Contributors
/// - Licensed under the MIT License
mod sync_wrapper {
    // This module contains a type that can make `Send + !Sync` types `Sync` by
    // disallowing all immutable access to the value.
    //
    // A similar primitive is provided in the `sync_wrapper` crate.

    use std::any::Any;

    pub(super) struct SyncWrapper<T> {
        value: T,
    }

    // safety: The SyncWrapper being send allows you to send the inner value across
    // thread boundaries.
    unsafe impl<T: Send> Send for SyncWrapper<T> {}

    // safety: An immutable reference to a SyncWrapper is useless, so moving such an
    // immutable reference across threads is safe.
    unsafe impl<T> Sync for SyncWrapper<T> {}

    impl<T> SyncWrapper<T> {
        pub(crate) fn new(value: T) -> Self {
            Self { value }
        }

        pub(crate) fn into_inner(self) -> T {
            self.value
        }
    }

    impl SyncWrapper<Box<dyn Any + Send>> {
        /// Attempt to downcast using `Any::downcast_ref()` to a type that is known to be `Sync`.
        pub(crate) fn downcast_ref_sync<T: Any + Sync>(&self) -> Option<&T> {
            // SAFETY: if the downcast fails, the inner value is not touched,
            // so no thread-safety violation can occur.
            self.value.downcast_ref()
        }
    }
}