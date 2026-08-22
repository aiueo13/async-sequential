use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};


pub struct OnceTake<T> {
    taken: AtomicBool,
    value: UnsafeCell<Option<T>>,
}

impl<T> OnceTake<T> {

    pub fn new(value: T) -> Self {
        Self {
            taken: AtomicBool::new(false),
            value: UnsafeCell::new(Some(value)),
        }
    }

    pub fn take(&self) -> Option<T> {
        if self.taken.swap(true, Ordering::Relaxed) {
            None
        } 
        else {
            // SAFETY:
            // Only the thread for which `swap` returned `false` can access `value`.
            unsafe { (*self.value.get()).take() }
        }
    }
}

unsafe impl<T: Send> Sync for OnceTake<T> {}