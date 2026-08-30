mod internal;
mod worker_handle;
mod poison_error;
mod task_canceller;
mod task_error;
mod task_handle;
mod task_spawner;

pub use worker_handle::WorkerHandle;
pub use poison_error::PoisonError;
pub use task_spawner::TaskSpawner;
pub use task_canceller::TaskCanceller;
pub use task_error::{TaskError, TaskErrorKind};
pub use task_handle::TaskHandle;


/// Spawns a worker with the given state,
/// returning its [WorkerHandle].
/// 
/// The worker runs asynchronous and blocking tasks sequentially on the state,
/// allowing the final state to be obtained after all tasks complete.
/// The worker does not provide an async runtime or its own thread pool. 
/// Internally, the worker runs as a single Tokio task on the current Tokio runtime.
/// Each blocking task is executed on Tokio's blocking thread pool.
/// 
/// Tasks are executed sequentially in the order they are queued,
/// regardless of whether they are asynchronous or blocking.
/// If a task panics, subsequent tasks also fail because the state invariants
/// may have been violated by the task's panic.
/// 
/// # Panics
/// Panics if called outside a Tokio runtime.
/// 
/// # Examples
/// ```
/// # fn main() {
/// # tokio_test::block_on(async {
/// use std::{thread, time::Duration};
/// use tokio::time::sleep;
/// 
/// // Spawn a worker with the given state
/// let worker = async_sequential::spawn_worker(Vec::new());
///
/// // Spawn a task onto the worker
/// worker.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
///     sleep(Duration::from_secs(1)).await;
///     state.push(1);
/// }));
///
/// // A spawner can be used to spawn tasks onto the worker
/// // from another thread or Tokio task.
/// let spawner = worker.spawner();
/// tokio::spawn(async move {
///     let task_handle1 = spawner.spawn_blocking(move |state| {
///         thread::sleep(Duration::from_secs(2));
///         state.push(2);
///         "hello"
///     });
///     let task_handle2 = spawner.spawn(move |state| Box::pin(async move {
///         sleep(Duration::from_secs(1)).await;
///         state.push(3);
///         "world"
///     }));
/// 
///     assert_eq!(task_handle1.await.unwrap(), "hello");
///     assert_eq!(task_handle2.await.unwrap(), "world");
/// 
///     // Drop the spawner to allow the worker to complete.
///     drop(spawner);
/// });
///
/// // Wait for all tasks to complete.
/// // NOTE: This does not complete as long as any spawner remains alive.
/// let result = worker.join().await.unwrap();
/// assert_eq!(result, vec![1, 2, 3]);
/// # });
/// # }
/// ``` 
/// 
/// [WorkerHandle]: crate::WorkerHandle
pub fn spawn_worker<S>(state: S) -> WorkerHandle<S>
where 
    S: Send + 'static
{
    WorkerHandle::new(internal::spawn_worker(
        state, 
        internal::WorkerRuntime::Current
    ))
}

/// Spawns a worker with the given state on the specified Tokio runtime,
/// returning its [WorkerHandle].
/// 
/// The worker runs asynchronous and blocking tasks sequentially on the state,
/// allowing the final state to be obtained after all tasks complete.
/// The worker does not provide an async runtime or its own thread pool. 
/// Internally, the worker runs as a single Tokio task on the specified Tokio runtime.
/// Each blocking task is executed on Tokio's blocking thread pool.
/// 
/// Tasks are executed sequentially in the order they are queued,
/// regardless of whether they are asynchronous or blocking.
/// If a task panics, subsequent tasks also fail because the state invariants
/// may have been violated by the task's panic.
/// 
/// Unlike [spawn_worker()], 
/// this function does not require the current thread to be running on a Tokio runtime.
/// 
/// [WorkerHandle]: crate::WorkerHandle
/// [spawn_worker()]: crate::spawn_worker
pub fn spawn_worker_on<S>(
    state: S,
    handle: &tokio::runtime::Handle
) -> WorkerHandle<S> 
where 
    S: Send + 'static
{
    WorkerHandle::new(internal::spawn_worker(
        state, 
        internal::WorkerRuntime::Handle(handle)
    ))
}


#[cfg(test)]
mod test_readme {
    use crate as async_sequential;
    use std::{thread, time::Duration};
    use tokio::time::sleep;
 
    #[tokio::test]
    async fn test() {
        // Spawn a worker with the given state
        let worker = async_sequential::spawn_worker(Vec::new());

        // Spawn a task onto the worker
        worker.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
            sleep(Duration::from_secs(1)).await;
            state.push(1);
        }));

        // A spawner can be used to spawn tasks onto the worker
        // from another thread or Tokio task.
        let spawner = worker.spawner();
        tokio::spawn(async move {
            let task_handle1 = spawner.spawn_blocking(move |state| {
                thread::sleep(Duration::from_secs(2));
                state.push(2);
                "hello"
            });
            let task_handle2 = spawner.spawn(move |state| Box::pin(async move {
                sleep(Duration::from_secs(1)).await;
                state.push(3);
                "world"
            }));
 
            assert_eq!(task_handle1.await.unwrap(), "hello");
            assert_eq!(task_handle2.await.unwrap(), "world");
 
            // Drop the spawner to allow the worker to complete.
            drop(spawner);
        });

        // Wait for all tasks to complete.
        // NOTE: This does not complete as long as any spawner remains alive.
        let result = worker.join().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }
}

#[cfg(test)]
mod tests4 {
    use std::thread;
    use tokio::{sync::oneshot, task::yield_now};

    #[test]
    fn test12() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = oneshot::channel();
        let (tx4, rx4) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let rh = runtime.handle().clone();

        let t = thread::spawn(move || {
            rx.recv().unwrap();
            runtime.block_on(async {
                rx2.await.unwrap();
            });
            drop(runtime)
        });

        let worker = crate::spawn_worker_on((), &rh);
        let task1 = worker.spawn(move |_| Box::pin(async {
            tx4.send(()).unwrap();
            rx3.await.unwrap();
        }));
        let task2 = worker.spawn_blocking(move |_| {});
        tx.send(()).unwrap();

        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            rx4.await.unwrap();
            tx2.send(()).unwrap();
            tx3.send(()).unwrap();

            t.join().unwrap();
            assert!(worker.cancel_and_join().await.unwrap_err().is_runtime_shutdown());

            let e = task1.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
            
            let e = task2.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
        });
    }

    #[test]
    fn test11() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = std::sync::mpsc::channel();
        let (tx4, rx4) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let rh = runtime.handle().clone();

        let t = thread::spawn(move || {
            rx.recv().unwrap();
            runtime.block_on(async {
                rx2.await.unwrap();
            });
            drop(runtime)
        });

        let worker = crate::spawn_worker_on((), &rh);
        let task1 = worker.spawn_blocking(move |_| {
            tx4.send(()).unwrap();
            rx3.recv().unwrap();
        });
        let task2 = worker.spawn_blocking(move |_| {});
        tx.send(()).unwrap();

        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            rx4.await.unwrap();
            tx2.send(()).unwrap();
            tx3.send(()).unwrap();

            t.join().unwrap();
            assert!(worker.abort_and_join().await.is_runtime_shutdown());

            assert!(task1.await.is_ok());

            let e = task2.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
        });
    }

    #[test]
    fn test10() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = oneshot::channel();
        let (tx4, rx4) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let rh = runtime.handle().clone();

        let t = thread::spawn(move || {
            rx.recv().unwrap();
            runtime.block_on(async {
                rx2.await.unwrap();
            });
            drop(runtime)
        });

        let worker = crate::spawn_worker_on((), &rh);
        let task1 = worker.spawn(move |_| Box::pin(async {
            tx4.send(()).unwrap();
            rx3.await.unwrap();
        }));
        let task2 = worker.spawn_blocking(move |_| {});
        tx.send(()).unwrap();

        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            rx4.await.unwrap();
            tx2.send(()).unwrap();
            tx3.send(()).unwrap();

            t.join().unwrap();
            assert!(worker.abort_and_join().await.is_runtime_shutdown());

            let e = task1.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
            
            let e = task2.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
        });
    }

    #[test]
    fn test9() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = oneshot::channel();
        let (tx4, rx4) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let rh = runtime.handle().clone();

        thread::spawn(move || {
            rx.recv().unwrap();
            runtime.block_on(async {
                rx2.await.unwrap();
            });
            drop(runtime)
        });

        let worker = crate::spawn_worker_on((), &rh);
        let task1 = worker.spawn(move |_| Box::pin(async {
            tx4.send(()).unwrap();
            rx3.await.unwrap();
        }));
        let task2 = worker.spawn_blocking(move |_| {});
        tx.send(()).unwrap();

        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            rx4.await.unwrap();
            tx2.send(()).unwrap();
            tx3.send(()).unwrap();

            assert!(worker.join().await.unwrap_err().is_runtime_shutdown());

            let e = task1.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
            
            let e = task2.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
        });
    }

    #[test]
    fn test8() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = std::sync::mpsc::channel();
        let (tx4, rx4) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let rh = runtime.handle().clone();

        thread::spawn(move || {
            rx.recv().unwrap();
            runtime.block_on(async {
                rx2.await.unwrap();
            });
            drop(runtime)
        });

        let worker = crate::spawn_worker_on((), &rh);
        let task1 = worker.spawn_blocking(move |_| {
            tx4.send(()).unwrap();
            rx3.recv().unwrap();
        });
        let task2 = worker.spawn(move |_| Box::pin(async {}));
        let task3 = worker.spawn_blocking(move |_| {});
        tx.send(()).unwrap();

        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            rx4.await.unwrap();
            tx2.send(()).unwrap();
            tx3.send(()).unwrap();

            assert!(worker.join().await.unwrap_err().is_runtime_shutdown());
            assert!(task1.await.is_ok());

            let e = task2.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());

            let e = task3.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
        });
    }

    #[test]
    fn test7() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let worker = crate::spawn_worker_on((), runtime.handle());
        drop(runtime);
        let task1 = worker.spawn_blocking(|_| {});

        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            let e = task1.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
            assert!(worker.join().await.unwrap_err().is_runtime_shutdown());
        });
    }

    #[test]
    fn test6() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (tx2, rx2) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let rh = runtime.handle().clone();

        thread::spawn(move || {
            rx.recv().unwrap();
            runtime.block_on(async {
                rx2.await.unwrap();
            })
        });

        let worker = crate::spawn_worker_on((), &rh);
        let task1 = worker.spawn_blocking(|_| {});
        tx.send(()).unwrap();

        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            assert!(task1.await.is_ok());
            assert!(worker.join().await.is_ok());
            tx2.send(()).unwrap();
        });
    }

    #[test]
    fn test5() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (tx2, rx2) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let rh = runtime.handle().clone();

        thread::spawn(move || {
            rx.recv().unwrap();
            runtime.block_on(async {
                rx2.await.unwrap();
            });
            drop(runtime)
        });

        let worker = crate::spawn_worker_on((), &rh);
        let task1 = worker.spawn(|_| Box::pin(async{
            yield_now().await;
        }));
        tx.send(()).unwrap();

        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            tx2.send(()).unwrap();
            let e = task1.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
            assert!(worker.join().await.unwrap_err().is_runtime_shutdown());
        });
    }

    #[test]
    fn test4() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let worker = crate::spawn_worker_on((), runtime.handle());
        drop(runtime);
        let task1 = worker.spawn(|_| Box::pin(async {}));

        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            let e = task1.await.unwrap_err();
            assert!(e.is_cancelled());
            assert!(!e.is_panic());
            assert!(e.kind().is_runtime_shutdown());
            assert!(worker.join().await.unwrap_err().is_runtime_shutdown());
        });
    }

    #[test]
    fn test3() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (tx2, rx2) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let rh = runtime.handle().clone();

        thread::spawn(move || {
            rx.recv().unwrap();
            runtime.block_on(async {
                rx2.await.unwrap();
            })
        });

        let worker = crate::spawn_worker_on((), &rh);
        let task1 = worker.spawn(|_| Box::pin(async{}));
        tx.send(()).unwrap();

        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            assert!(task1.await.is_ok());
            assert!(worker.join().await.is_ok());
            tx2.send(()).unwrap();
        });
    }

    #[tokio::test]
    async fn test2() {
        let worker = crate::spawn_worker(Vec::new());
        let task1 = worker.spawn_blocking(|s| {
            s.push(0);
        });
        let task2 = worker.spawn_blocking(|s| {
            s.push(1);
            panic!("task panic")
        });
        let task3 = worker.spawn_blocking(|s| {
            s.push(2);
        });

        assert!(task1.await.is_ok());
        let e = task2.await.unwrap_err();
        assert!(e.kind().is_task_panic());
        assert!(e.is_panic());
        assert!(!e.is_cancelled());
        let e = task3.await.unwrap_err();
        assert!(e.kind().is_previous_task_panic());
        assert!(!e.is_panic());
        assert!(e.is_cancelled());

        assert_eq!(worker.join().await.unwrap_err().into_inner(), vec![0, 1])
    }

    #[tokio::test]
    async fn test1() {
        let worker = crate::spawn_worker(Vec::new());
        let task1 = worker.spawn(|s| Box::pin(async {
            s.push(0);
        }));
        let task2 = worker.spawn(|s| Box::pin(async {
            s.push(1);
            panic!("task panic")
        }));
        let task3 = worker.spawn(|s| Box::pin(async {
            s.push(2);
        }));

        assert!(task1.await.is_ok());
        let e = task2.await.unwrap_err();
        assert!(e.kind().is_task_panic());
        assert!(e.is_panic());
        assert!(!e.is_cancelled());
        let e = task3.await.unwrap_err();
        assert!(e.kind().is_previous_task_panic());
        assert!(!e.is_panic());
        assert!(e.is_cancelled());

        assert_eq!(worker.join().await.unwrap_err().into_inner(), vec![0, 1])
    }
}

#[cfg(test)]
mod tests3 {
    use std::{future::pending, sync::Arc, time::Duration};
    use tokio::{sync::{mpsc, oneshot}, time::sleep};


    #[tokio::test]
    async fn test1() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();

        let handle = worker.spawn(|_| Box::pin(async {
            rx.await.unwrap();
            42
        }));

        tx.send(()).unwrap();
        assert_eq!(handle.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test2() {
        let worker = crate::spawn_worker(());

        let handle = worker.spawn(|_| Box::pin(async {
            42
        }));

        worker.join().await.unwrap();
        assert_eq!(handle.await.unwrap(), 42);
    }


    #[tokio::test]
    async fn test3() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();

        let running = worker.spawn(|_| Box::pin(async {
            rx.await.unwrap();
        }));
        let handle = worker.spawn(|_| Box::pin(async {
            42
        }));

        assert!(handle.cancel());
        assert!(handle.await.unwrap_err().is_cancelled());

        // The running task must not be affected by cancellation of the queued task.
        tx.send(()).unwrap();
        assert!(running.await.is_ok());
    }


    #[tokio::test]
    async fn test4() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let handle = worker.spawn(|_| Box::pin(async {
            tx2.send(()).unwrap();
            rx.await.unwrap();
            42
        }));

        rx2.await.unwrap();
        assert!(!handle.cancel());

        tx.send(()).unwrap();
        assert_eq!(handle.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test5() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();

        let running = worker.spawn(|_| Box::pin(async {
            rx.await.unwrap();
        }));
        let handle = worker.spawn(|_| Box::pin(async {
            42
        }));

        assert!(handle.cancel());
        assert!(!handle.cancel());

        tx.send(()).unwrap();
        assert!(handle.await.unwrap_err().is_cancelled());
        assert!(running.await.is_ok());
    }


    #[tokio::test]
    async fn test6() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let handle = worker.spawn(|_| Box::pin(async {
            tx2.send(()).unwrap();
            rx.await.unwrap();
            42
        }));

        rx2.await.unwrap();

        worker.abort();

        assert!(!handle.cancel());

        tx.send(()).unwrap();
        assert!(handle.await.unwrap_err().is_cancelled());
    }


    #[tokio::test]
    async fn test7() {
        let worker = crate::spawn_worker(());

        let handle = worker.spawn(|_| Box::pin(async {
            panic!("task panic");
        }));

        assert!(handle.await.unwrap_err().is_panic());
    }


    #[tokio::test]
    async fn test8() {
        let worker = crate::spawn_worker(());

        let first = worker.spawn(|_| Box::pin(async {
            panic!("first task panic");
        }));

        assert!(first.await.is_err());

        let second = worker.spawn(|_| Box::pin(async {
            42
        }));

        assert!(!second.cancel());
        let err = second.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(err.kind().is_previous_task_panic());
    }


    #[tokio::test]
    async fn test9() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let handle = worker.spawn(|_| Box::pin(async {
            tx2.send(()).unwrap();
            rx.await.unwrap();
        }));

        rx2.await.unwrap();
        tx.send(()).unwrap();
        assert!(handle.await.is_ok());
    }

    #[tokio::test]
    async fn test10() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();

        let running = worker.spawn(|_| Box::pin(async {
            rx.await.unwrap();
        }));

        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();

        let handle = worker.spawn(move |_| Box::pin(async move {
            executed_clone.store(true, Ordering::SeqCst);
        }));

        assert!(handle.cancel());
        assert!(handle.await.unwrap_err().is_cancelled());

        tx.send(()).unwrap();
        running.await.unwrap();

        assert!(!executed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test11() {
        let worker = crate::spawn_worker(());

        let handle = worker.spawn(|_| Box::pin(async {
            panic!()
        }));

        let _ = worker.spawn(|_| Box::pin(async {})).await;

        worker.abort();
        let err = handle.await.unwrap_err();
        assert!(err.is_panic());
        assert!(!err.is_cancelled());
    }

    #[tokio::test]
    async fn test12() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();

        let handle = worker.spawn(|_| Box::pin(async {
            rx.await.unwrap();
            panic!()
        }));

        worker.abort();
        tx.send(()).unwrap();
        let err = handle.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
    }

    #[tokio::test]
    async fn test13() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
    
        worker.spawn(move |_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));

        let handle = worker.spawn(move |_| Box::pin(async {}));

        rx.await.unwrap();
        handle.cancel();
        let err = handle.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(!err.is_panic());
    }

    #[tokio::test]
    async fn test14() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();

        let handle1 = worker.spawn(move |_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let handle2 = worker.spawn(move |_| Box::pin(async {}));

        rx.await.unwrap();
        worker.abort();

        let err1 = handle1.await.unwrap_err();
        assert!(err1.is_cancelled());
        assert!(!err1.is_panic());
        let err2 = handle2.await.unwrap_err();
        assert!(err2.is_cancelled());
        assert!(!err2.is_panic());
    }

    #[tokio::test]
    async fn test15() {
        let worker = crate::spawn_worker(Vec::new());
        let (tx, rx) = oneshot::channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();

        let handle1 = worker.spawn(move |state| Box::pin(async move {
            tx.send(()).unwrap();
            while let Some(v) = rx2.recv().await {
                state.push(v);
            }
            state.clone()
        }));
        let handle2 = worker.spawn(move |_| Box::pin(async {
            pending::<()>().await;
        }));

        tx2.send(0).unwrap();
        rx.await.unwrap();
        
        worker.cancel();

        let err2 = handle2.await.unwrap_err();
        assert!(err2.is_cancelled());
        assert!(!err2.is_panic());

        tx2.send(1).unwrap();
        drop(tx2);

        assert_eq!(handle1.await.unwrap(), vec![0, 1]);
    }

    #[tokio::test]
    async fn test16() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();

        let running = worker.spawn(move |_| Box::pin(async move {
            tx.send(()).unwrap();
            sleep(Duration::from_millis(500)).await;
            "complete"
        }));

        let pending = worker.spawn(move |_| Box::pin(async move { }));

        rx.await.unwrap();
        worker.cancel_and_join().await.unwrap();
        assert_eq!(running.await.unwrap(), "complete");
        assert!(pending.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn test17() {
        let worker = crate::spawn_worker(());

        let handle = worker.spawn(move |_| Box::pin(async move {
            panic!();
        }));
        let err = handle.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.kind().is_task_panic());
        assert!(!err.kind().is_previous_task_panic());
        assert!(!err.is_cancelled());

        let handle = worker.spawn(move |_| Box::pin(async move {}));
        let err = handle.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.kind().is_previous_task_panic());
        assert!(!err.kind().is_task_panic());
        assert!(err.is_cancelled());
    }

    #[tokio::test]
    async fn test18() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
        let _ = worker.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let pending = worker.spawn(|_| Box::pin(async {

        }));
        rx.await.unwrap();

        pending.cancel();
        drop(worker);

        let err = pending.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(err.kind().is_task_cancelled());
        assert!(!err.kind().is_worker_aborted());
        assert!(!err.kind().is_worker_cancelled());
    }

    #[tokio::test]
    async fn test19() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
        let _ = worker.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let pending = worker.spawn(|_| Box::pin(async {

        }));
        rx.await.unwrap();

        pending.canceller().cancel();
        drop(worker);

        let err = pending.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(err.kind().is_task_cancelled());
        assert!(!err.kind().is_worker_aborted());
        assert!(!err.kind().is_worker_cancelled());
    }

    #[tokio::test]
    async fn test20() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
        let _ = worker.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let pending = worker.spawn(|_| Box::pin(async {

        }));
        rx.await.unwrap();

        worker.abort();
        let err = pending.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(!err.kind().is_task_cancelled());
        assert!(err.kind().is_worker_aborted());
        assert!(!err.kind().is_worker_cancelled());
    }

    #[tokio::test]
    async fn test21() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
        let _ = worker.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let pending = worker.spawn(|_| Box::pin(async {

        }));
        rx.await.unwrap();

        worker.cancel();
        let err = pending.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(!err.kind().is_task_cancelled());
        assert!(!err.kind().is_worker_aborted());
        assert!(err.kind().is_worker_cancelled());
    }

    #[tokio::test]
    async fn test22() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
        let _ = worker.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            panic!()
        }));
        let pending = worker.spawn(|_| Box::pin(async {

        }));
        rx.await.unwrap();

        worker.cancel();
        let err = pending.await.unwrap_err();
        assert!(!err.kind().is_task_panic());
        assert!(err.kind().is_previous_task_panic());
        assert!(err.is_cancelled());
    }

    #[tokio::test]
    async fn test23() {
        let worker = crate::spawn_worker(0);

        for _ in 0..10000 {
            worker.spawn(|s| Box::pin(async {
                *s += 1;
            })).await.unwrap();

            worker.spawn_blocking(|s| {
                *s += 1;
            }).await.unwrap();
        }
        
        assert_eq!(worker.join().await.unwrap(), 20000);
    }

    #[tokio::test]
    async fn test24() {
        let worker = crate::spawn_worker(());
        let h = worker.spawn(|_| Box::pin(async { panic!()}));
        assert!(h.await.unwrap_err().kind().is_task_panic());
    }

    #[tokio::test]
    async fn test25() {
        let worker = crate::spawn_worker(());
        let h = worker.spawn_blocking(|_| { panic!() });
        assert!(h.await.unwrap_err().kind().is_task_panic());
    }

    #[tokio::test]
    async fn test26() {
        let worker = crate::spawn_worker(());
        assert!(!worker.has_panicked());
        let _ = worker.spawn_blocking(|_| { }).await; 
        assert!(!worker.has_panicked());
        let h = worker.spawn_blocking(|_| { panic!() }); 
        let r = h.await;
        assert!(worker.has_panicked());
        assert!(r.unwrap_err().kind().is_task_panic());

        let worker = crate::spawn_worker(());
        let s = worker.spawner();
        let h = s.spawn_blocking(|_| { panic!() }); 
        let r = h.await;
        assert!(s.has_panicked().unwrap());
        assert!(worker.has_panicked());
        assert!(r.unwrap_err().kind().is_task_panic());
    }

    #[tokio::test]
    async fn test27() {
        let worker = crate::spawn_worker(Vec::new());
        let spawner = worker.spawner();
        
        std::thread::spawn(move || {
            assert!(tokio::runtime::Handle::try_current().unwrap_err().is_missing_context());

            spawner.spawn(|s: &mut Vec<_>| Box::pin(async {
                s.push(0);
            }));
            spawner.spawn_blocking(|s: &mut Vec<_>| {
                s.push(1);
            });
        }).join().unwrap();

        assert_eq!(worker.join().await.unwrap(), vec![0, 1]);
    }

    #[tokio::test]
    async fn test28() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
        let h = worker.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            sleep(Duration::from_secs(1)).await;
            panic!()
        }));
        rx.await.unwrap();

        assert!(worker.cancel_and_join().await.is_err());
        assert!(h.await.unwrap_err().kind().is_task_panic());
    }

    #[tokio::test]
    async fn test29() {
        let worker = crate::spawn_worker(());
        let (tx, rx) = oneshot::channel();
        let h = worker.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            sleep(Duration::from_secs(1)).await;
            panic!()
        }));
        rx.await.unwrap();

        assert!(worker.join().await.is_err());
        assert!(h.await.unwrap_err().kind().is_task_panic());
    }

    #[tokio::test]
    async fn test30() {
        let se = crate::spawn_worker(Vec::<&'static str>::new());
        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));
        assert!(se.join().await.unwrap_err().is_task_panic());
    }
}

#[cfg(test)]
mod tests2 {
    use tokio::sync::oneshot;


    #[tokio::test]
    async fn test() {
        let worker = crate::spawn_worker(Vec::new());

        worker.spawn(move |state| Box::pin(async move {
            state.push(identity("1").await);
        }));

        let spawner = worker.spawner();
        tokio::spawn(async move {
            spawner.spawn_blocking(move |state| {
                state.push("2");
            });

            let task_handle = spawner.spawn(move |state| Box::pin(async move {
                state.push("3");
                "hello world"
            }));
            assert_eq!(task_handle.await.unwrap(), "hello world");
        });

        // Wait for all tasks to complete.
        // NOTE: This does not complete as long as `spawner` has not been dropped.
        let result = worker.join().await.unwrap();
        assert_eq!(result, vec!["1", "2", "3"]);

        async fn identity<T>(v: T) -> T { v }
    }

    #[tokio::test]
    async fn test0() {
        let worker = crate::spawn_worker(Vec::new());

        worker.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
            state.push(identity(0).await);
        }));

        worker.spawn_blocking(move |state| {
            state.push(1);
        });

        let task_result = worker.spawn(move |state| Box::pin(async move {
            state.push(identity(2).await);
            "hello"
        })).await.unwrap();
        assert_eq!(task_result, "hello");

        let task_result = worker.spawn_blocking(move |state| {
            state.push(3);
            "world"
        }).await.unwrap();
        assert_eq!(task_result, "world");

        let result = worker.join().await;
        assert_eq!(result.unwrap(), vec![0, 1, 2, 3]);

        async fn identity(v: u64) -> u64 {
            v
        }
    }

    #[tokio::test]
    async fn test1() {
        let worker = crate::spawn_worker(Vec::new());
        let c = 1000;

        for i in 0..c {
            if i % 2 == 0 {
                worker.spawn(move |state| Box::pin(async move { state.push(i); }));
            }
            else {
                worker.spawn_blocking(move |state| { state.push(i); });
            }
        }

        assert_eq!(worker.join().await.unwrap(), (0..c).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test2() {
        let worker = crate::spawn_worker(Vec::new());
        let c = 1000;

        for i in 0..c {
            if i % 2 == 0 {
                let r = worker.spawn(move |state| Box::pin(async move { 
                    state.push(i); 
                    i
                })).await;

                assert_eq!(r.unwrap(), i);
            }
            else {
                let r = worker.spawn_blocking(move |state| { 
                    state.push(i); 
                    i
                }).await;

                assert_eq!(r.unwrap(), i);
            }
        }

        assert_eq!(worker.join().await.unwrap(), (0..c).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test3() {
        let worker = crate::spawn_worker(Vec::new());
        let c = 1000;

        for i in 0..c {
            if i % 2 == 0 {
                worker.spawn(move |state| Box::pin(async move { state.push(i); })).await.unwrap();
            }
            else {
                worker.spawn_blocking(move |state| { state.push(i); }).await.unwrap();
            }
        }

        assert_eq!(worker.join().await.unwrap(), (0..c).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test4() {
        let worker = crate::spawn_worker(vec![0]);
        assert_eq!(worker.join().await.unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn test5() {
        let worker = crate::spawn_worker(vec![0]);
        assert_eq!(worker.join().await.unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn test6() {
        let worker = crate::spawn_worker(vec![0]);
        worker.spawn(|_| Box::pin(async { panic!() }));
        let r = worker.join().await;
        assert!(r.as_ref().is_err());
        assert!(r.as_ref().is_err());
    }

    #[tokio::test]
    async fn test7() {
        {
            let worker = crate::spawn_worker(());
        
            let (tx, rx) = oneshot::channel();
            let handle = worker.spawn(move |_| Box::pin(async {
                rx.await.unwrap();
            }));

            worker.abort();
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
        {
            let worker = crate::spawn_worker(());
        
            let (tx, rx) = oneshot::channel();
            let handle = worker.spawn_blocking(move |_| {
                rx.blocking_recv().unwrap();
            });

            worker.abort();
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
    }

    #[tokio::test]
    async fn test8() {
        {
            let worker = crate::spawn_worker(());
        
            let handle = worker.spawn(move |_| Box::pin(async {
                panic!()
            }));

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| !e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.is_panic()));
            assert!(r.as_ref().is_err_and(|e| e.kind().is_task_panic()));
        }
        {
            let worker = crate::spawn_worker(());
        
            let handle = worker.spawn_blocking(move |_| {
                panic!("this is a panic message")
            });

            let r = handle.await;
            eprintln!("{}", r.as_ref().unwrap_err());
            assert!(r.as_ref().is_err_and(|e| !e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.is_panic()));
            assert!(r.as_ref().is_err_and(|e| e.kind().is_task_panic()));
        }
    }

    #[tokio::test]
    async fn test9() {
        {
            let worker = crate::spawn_worker(());
        
            let (tx, rx) = oneshot::channel();
            let handle = worker.spawn(move |_| Box::pin(async {
                rx.await.unwrap();
                panic!()
            }));

            worker.abort();
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
        {
            let worker = crate::spawn_worker(());
        
            let (tx, rx) = oneshot::channel();
            let handle = worker.spawn_blocking(move |_| {
                rx.blocking_recv().unwrap();
                panic!()
            });

            worker.abort();
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
    }

    #[tokio::test]
    async fn test10() {
        {
            let worker = crate::spawn_worker(());
        
            worker.spawn(move |_| Box::pin(async {
                panic!()
            }));
            let handle = worker.spawn(move |_| Box::pin(async {
                
            }));

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.kind().is_previous_task_panic()));
        }
        {
            let worker = crate::spawn_worker(());
        
            worker.spawn_blocking(move |_| {
                panic!()
            });
            let handle = worker.spawn_blocking(move |_| {
                
            });

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.kind().is_previous_task_panic()));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[tokio::test]
    async fn test1() {
        let se = crate::spawn_worker("0".to_string());

        se.spawn_blocking(|ctx| {
            if ctx == "0" {
                *ctx = "1".into();
            }
        }).await.ok();
        se.spawn(|ctx| Box::pin(async move {
            if ctx == "1" {
                *ctx = "2".into();
            }
        })).await.ok();
        se.spawn(|ctx| Box::pin(async move {
            if ctx == "2" {
                *ctx = "3".into();
            }
        })).await.ok();

        let result = se.join().await;

        assert_eq!(&result.unwrap(), "3")
    }

    #[tokio::test]
    async fn test2() {
        let se = crate::spawn_worker(0);

        #[allow(unused_must_use)] {
        tokio::join!(
            se.spawn(|ctx| Box::pin(async{*ctx += 1;})),
            se.spawn(|ctx| Box::pin(async{*ctx += 1;})),
            se.spawn(|ctx| Box::pin(async{*ctx += 1;})),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
        );
        }

        let result = se.join().await.unwrap();

        assert_eq!(result, 9)
    }

    #[tokio::test]
    async fn test3() {
        let se = Arc::new(crate::spawn_worker(0));

        let mut set = tokio::task::JoinSet::new();
        let i = 10000;
        for _ in 0..i {
            let se1 = Arc::clone(&se);
            let se2 = Arc::clone(&se);
            set.spawn(async move {
                se1.spawn(|ctx| Box::pin(async{*ctx += 1;})).await.unwrap();
            });
            set.spawn(async move {
                se2.spawn_blocking(|ctx| {*ctx += 1;}).await.unwrap();
            });
        }

        set.join_all().await;

        let result = Arc::into_inner(se).unwrap().join().await;

        assert_eq!(result.unwrap(), i * 2)
    }

    #[tokio::test]
    async fn test4() {
        let se = crate::spawn_worker(0);
        let result = se.join().await;
        assert_eq!(result.unwrap(), 0)
    }

    #[tokio::test]
    #[should_panic]
    async fn test6() {
        let se = crate::spawn_worker(Vec::<&'static str>::new());

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        })).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test7() {
        let se = crate::spawn_worker(Vec::<&'static str>::new());

        se.spawn_blocking(|_ctx| {
            panic!()
        }).await.unwrap();
    }

    #[tokio::test]
    async fn test8() {
        let se = crate::spawn_worker(Vec::<&'static str>::new());

        // TaskHandle を待機しないと task で panic しても sumit_blocking は panic しない
        se.spawn_blocking(|_ctx| {
            panic!()
        });
    }

    #[tokio::test]
    async fn test9() {
        let se = crate::spawn_worker(Vec::<&'static str>::new());

        // TaskHandle を待機しないと task で panic しても sumit は panic しない
        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));
    }

    #[tokio::test]
    #[should_panic]
    async fn test10() {
        let se = crate::spawn_worker(Vec::<&'static str>::new());

        se.spawn_blocking(|_ctx| {
            panic!()
        }).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test11() {
        let se = crate::spawn_worker(Vec::<&'static str>::new());

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        })).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test12() {
        let se = crate::spawn_worker(Vec::<&'static str>::new());

        // task が panic　した場合、その後の task も panic　になる。

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));

        se.spawn_blocking(|_ctx| {}).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test13() {
        let se = crate::spawn_worker(Vec::<&'static str>::new());

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));

        se.join().await.unwrap();
    }

    #[tokio::test]
    async fn test14() {
        let se = crate::spawn_worker(Vec::<u64>::new());
        let i = 1000;

        for i in 0..i {
            se.spawn(move |ctx| Box::pin(async move {
                ctx.push(i);
            }));
        }

        assert_eq!(se.join().await.unwrap(), (0..i).collect::<Vec<_>>())
    }

    #[tokio::test]
    async fn test15() {
        let se = crate::spawn_worker(Vec::<u64>::new());
        let s = tokio::sync::Mutex::new(0);

        se.spawn(move |ctx| Box::pin(async move {
            ctx.push({
                let mut s = s.lock().await;
                *s += 1;
                *s
            });
            ctx.push({
                let mut s = s.lock().await;
                *s += 1;
                *s
            });
            ctx.push({
                let mut s = s.lock().await;
                *s += 1;
                *s
            });
        }));

        assert_eq!(se.join().await.unwrap(), vec![1, 2, 3])
    }

    #[tokio::test]
    #[should_panic]
    async fn test16() {
        let se = crate::spawn_worker(());
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = se.spawn(|_| Box::pin(async {
            let _ = rx.await;
        }));
        se.abort();
        let _ = tx.send(());
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test17() {
        let worker = crate::spawn_worker(Vec::<u64>::new());

        let handle = worker.spawn(|state| Box::pin(async move {
            state.push(1);
        }));

        drop(handle);

        let result = worker.join().await;

        assert_eq!(result.unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn test18() {
        let worker = crate::spawn_worker(Vec::<&'static str>::new());

        worker.spawn(|state| Box::pin(async move {
            state.push("async-1");
            tokio::task::yield_now().await;
            state.push("async-2");
        }));

        worker.spawn_blocking(|state| {
            state.push("blocking");
        });

        worker.spawn(|state| Box::pin(async move {
            state.push("async-3");
        }));

        assert_eq!(
            worker.join().await.unwrap(),
            vec![
                "async-1",
                "async-2",
                "blocking",
                "async-3",
            ]
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn test19() {
        let worker = crate::spawn_worker(());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let handle = worker.spawn(move |_| Box::pin(async move {
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));

        started_rx.await.unwrap();

        worker.abort();

        handle.await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test20() {
        let worker = crate::spawn_worker(Vec::<u64>::new());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        worker.spawn(move |state| Box::pin(async move {
            state.push(1);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));

        started_rx.await.unwrap();

        let queued_handle = worker.spawn(|state| Box::pin(async move {
            state.push(2);
        }));

        worker.abort();

        queued_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test21() {
        let worker = crate::spawn_worker(Vec::<u64>::new());

        for i in 0..1000 {
            worker.spawn(move |state| Box::pin(async move {
                state.push(i);
            }));
        }

        let result = worker.join().await.unwrap();

        assert_eq!(result, (0..1000).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test22() {
        let se = crate::spawn_worker(0);
        let c = 4;

        for _ in 0..c {
            se.spawn(|s| Box::pin(async {
                *s += 1;
                tokio::time::sleep(std::time::Duration::from_secs(1))
            }));  
        }

        let result = se.join().await;
        assert_eq!(result.unwrap(), c)
    }
}