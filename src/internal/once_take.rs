use std::cell::UnsafeCell;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};


pub struct OnceTake<T> {
    taken: AtomicBool,
    value: UnsafeCell<Option<T>>,
}

impl<T> OnceTake<T> {

    pub const fn new(value: T) -> Self {
        Self {
            taken: AtomicBool::new(false),
            value: UnsafeCell::new(Some(value)),
        }
    }

    pub const fn empty() -> Self {
        Self {
            taken: AtomicBool::new(true),
            value: UnsafeCell::new(None),
        }
    }

    pub fn take(&self) -> Option<T> {
        if self.taken.swap(true, Ordering::AcqRel) {
            None
        } 
        else {
            // SAFETY:
            // `taken` を true に変更できたスレッドだけが `value` にアクセスする。
            unsafe { (*self.value.get()).take() }
        }
    }
}

impl<T> From<Option<T>> for OnceTake<T> {

    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => Self::new(value),
            None => Self::empty(),
        }
    }
}

impl<T> RefUnwindSafe for OnceTake<T> {}
impl<T> UnwindSafe for OnceTake<T> {}

// SAFETY:
// `taken` を true に変更できたスレッドだけが `value` にアクセスできる。
// そのため、`value` が複数のスレッドから同時にアクセスされることはなく、
// `T: Send` の場合に限り `OnceTake<T>` を複数スレッド間で共有できる。
unsafe impl<T: Send> Sync for OnceTake<T> {}

// SAFETY:
// `taken` を true に変更できたスレッドだけが `value` を取得できる。
unsafe impl<T: Send> Send for OnceTake<T> {}