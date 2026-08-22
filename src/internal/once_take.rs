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
            // `taken` を true に変更できたスレッドだけが `value` にアクセスする。
            unsafe { (*self.value.get()).take() }
        }
    }
}

// SAFETY:
// `taken` を true に変更できたスレッドだけが `value` にアクセスする。
// そのため、`value` が複数のスレッドから同時にアクセスされることはなく、
// `T: Send` の場合に限り `OnceTake<T>` を複数スレッド間で共有できる。
unsafe impl<T: Send> Sync for OnceTake<T> {}