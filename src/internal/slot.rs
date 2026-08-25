use std::{ptr, sync::{Arc, atomic::{AtomicPtr, Ordering}}};


pub fn slot<T>() -> (SlotSetter<T>, SlotTaker<T>) {
    let state = Arc::new(State { ptr: AtomicPtr::new(ptr::null_mut()) });
    let setter = SlotSetter { state: Arc::clone(&state) };
    let taker = SlotTaker { state };
    (setter, taker)
}

struct State<T> {
    ptr: AtomicPtr<T>,
}

impl<T> Drop for State<T> {

    fn drop(&mut self) {
        let ptr = self.ptr.load(Ordering::Relaxed);
        if !ptr.is_null() {
            // SAFETY:
            // ptr は Box::into_raw によって生成されたポインタであり、現在はこの State が所有している。
            // State が破棄される時点ではこれを参照する Setter と Taker はすべて破棄されているため、
            // ptr に対する操作と並行して実行されることはない。
            // したがって Box::from_raw によって所有権を復元して破棄しても安全。
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
    }
}

pub struct SlotSetter<T> {
    state: Arc<State<T>>,
}

impl<T: Send> SlotSetter<T> {

    pub fn set(&self, value: T) {
        let new = Box::into_raw(Box::new(value));
        let old = self.state.ptr.swap(new, Ordering::AcqRel);
        if !old.is_null() {
            // SAFETY:
            // old は swap によって AtomicPtr から取り出されたポインタである。
            // AtomicPtr に格納される非 null ポインタはすべて Box::into_raw によって生成されており、
            // swap によってこの処理がそのポインタの所有権を取得する。
            // ポインタの取得と置換は swap によって原子的に行われるため、
            // 他の処理が同じポインタの所有権を取得することはない。
            // したがって Box::from_raw によって所有権を復元して破棄しても安全。
            unsafe {
                drop(Box::from_raw(old)); 
            }
        }
    }
}

pub struct SlotTaker<T> {
    state: Arc<State<T>>,
}

impl<T: Send> SlotTaker<T> {

    pub fn take(&self) -> Option<T> {
        let ptr = self.state.ptr.swap(ptr::null_mut(), Ordering::AcqRel);
        if ptr.is_null() {
            None
        }
        else {
            // SAFETY:
            // ptr は swap によって AtomicPtr から原子的に取り出されたポインタである。
            // AtomicPtr に格納される非 null ポインタはすべて Box::into_raw によって生成されている。
            // swap によってポインタが AtomicPtr から取り除かれたため、
            // この処理がそのポインタの唯一の所有権を取得している。
            // したがって Box::from_raw によって所有権を復元し、その中の値を取り出しても安全。
            Some(unsafe { 
                *Box::from_raw(ptr) 
            })
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::task;

    #[test]
    fn take_returns_none_when_empty() {
        let (_setter, taker) = slot::<usize>();

        assert_eq!(taker.take(), None);
    }

    #[test]
    fn set_then_take_returns_value() {
        let (setter, taker) = slot();

        setter.set(42);

        assert_eq!(taker.take(), Some(42));
        assert_eq!(taker.take(), None);
    }

    #[test]
    fn set_overwrites_previous_value() {
        let (setter, taker) = slot();

        setter.set(1);
        setter.set(2);

        assert_eq!(taker.take(), Some(2));
        assert_eq!(taker.take(), None);
    }

    #[test]
    fn set_after_take_works() {
        let (setter, taker) = slot();

        setter.set(1);
        assert_eq!(taker.take(), Some(1));

        setter.set(2);
        assert_eq!(taker.take(), Some(2));
    }

    #[test]
    fn take_removes_value() {
        let (setter, taker) = slot();

        setter.set(42);

        assert_eq!(taker.take(), Some(42));
        assert_eq!(taker.take(), None);
    }

    #[test]
    fn value_is_dropped_when_overwritten() {
        struct DropCounter(Arc<AtomicUsize>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let (setter, taker) = slot();

        setter.set(DropCounter(Arc::clone(&drops)));
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        setter.set(DropCounter(Arc::clone(&drops)));
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        drop(taker);
        drop(setter);

        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn value_is_dropped_when_state_is_dropped() {
        struct DropCounter(Arc<AtomicUsize>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));

        {
            let (setter, _taker) = slot();

            setter.set(DropCounter(Arc::clone(&drops)));

            assert_eq!(drops.load(Ordering::Relaxed), 0);
        }

        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn taken_value_is_not_dropped_twice() {
        struct DropCounter(Arc<AtomicUsize>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));

        {
            let (setter, taker) = slot();

            setter.set(DropCounter(Arc::clone(&drops)));

            let value = taker.take().unwrap();
            assert_eq!(drops.load(Ordering::Relaxed), 0);

            drop(value);
            assert_eq!(drops.load(Ordering::Relaxed), 1);

            drop(setter);
            drop(taker);
        }

        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn setter_and_taker_are_independently_droppable() {
        let drops = Arc::new(AtomicUsize::new(0));

        struct DropCounter(Arc<AtomicUsize>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let (setter, taker) = slot();

        setter.set(DropCounter(Arc::clone(&drops)));

        drop(setter);

        assert_eq!(drops.load(Ordering::Relaxed), 0);
        assert_eq!(taker.take().is_some(), true);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn concurrent_set_and_take() {
        let (setter, taker) = slot();

        let setter = task::spawn_blocking(move || {
            for i in 0..100_000 {
                setter.set(i);
            }
        });

        let taker = task::spawn_blocking(move || {
            let mut count = 0;

            for _ in 0..100_000 {
                if taker.take().is_some() {
                    count += 1;
                }
            }

            count
        });

        setter.await.unwrap();
        let _ = taker.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_set_and_take_preserves_memory_safety() {
        let drops = Arc::new(AtomicUsize::new(0));

        struct DropCounter(Arc<AtomicUsize>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let (setter, taker) = slot();

        let setter_drops = Arc::clone(&drops);
        let setter = task::spawn_blocking(move || {
            for _ in 0..100_000 {
                setter.set(DropCounter(Arc::clone(&setter_drops)));
            }
        });

        let taker = task::spawn_blocking(move || {
            for _ in 0..100_000 {
                let _ = taker.take();
            }
        });

        setter.await.unwrap();
        taker.await.unwrap();

        // setter と taker が終了した後、残っている値があれば State の Drop で破棄される。
        // したがって生成した全値が最終的に一度だけ Drop される。
        assert_eq!(drops.load(Ordering::Relaxed), 100_000);
    }
}