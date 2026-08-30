use crate::*;
use std::pin::Pin;


/// A handle for spawning tasks onto the associated worker.
///
/// This TaskSpawner can be obtained from [WorkerHandle::spawner()]
/// 
/// Note that [WorkerHandle::join()]
/// does not complete as long as any TaskSpawner remains alive.
/// To allow it to complete, 
/// either drop all TaskSpawners or call [WorkerHandle::close_spawners()] beforehand.
/// 
/// [WorkerHandle::spawner()]: crate::WorkerHandle::spawner
/// [WorkerHandle::join()]: crate::WorkerHandle::join
/// [WorkerHandle::close_spawners()]: crate::WorkerHandle::close_spawners
pub struct TaskSpawner<S> {
    repr: internal::WorkerTaskSender<S>,
}

impl<S> TaskSpawner<S> {

    pub(crate) fn new(repr: internal::WorkerTaskSender<S>) -> Self {
        Self { repr }
    }
}

impl<S> TaskSpawner<S> {

    /// Returns true if this TaskSpawner can no longer spawn tasks.
    ///
    /// This occurs when the TaskSpawner has been closed
    /// or the associated worker has been aborted or cancelled.
    /// From this point onward,
    /// [spawn()] or [spawn_blocking()] returns a [TaskHandle]
    /// that immediately resolves to an error
    /// for which [TaskError::kind()] is [TaskErrorKind::TaskSpawnerUnavailable].
    /// 
    /// [spawn()]: Self::spawn
    /// [spawn_blocking()]: Self::spawn_blocking
    /// [TaskHandle]: crate::TaskHandle
    /// [TaskError::kind()]: crate::TaskError::kind
    /// [TaskErrorKind::TaskSpawnerUnavailable]: crate::TaskErrorKind::TaskSpawnerUnavailable
    pub fn is_unavailable(&self) -> bool {
        self.repr.is_unavailable()
    }

    /// Returns true if a task executed by the associated worker has panicked.
    /// 
    /// Once a task has panicked, while this TaskSpawner is not unavailable,
    /// [spawn()] or [spawn_blocking()] returns a [TaskHandle]
    /// that immediately resolves to an error
    /// for which [TaskError::kind()] is [TaskErrorKind::PreviousTaskPanic].
    /// This is because the state invariants may have been violated by the task's panic.
    /// 
    /// Returns None if the TaskSpawner is unavailable.
    /// This occurs when the TaskSpawner has been closed
    /// or the worker has been aborted or cancelled.
    /// From this point onward,
    /// spawn() or spawn_blocking() returns a TaskHandle
    /// that immediately resolves to an error
    /// for which TaskError::kind() is [TaskErrorKind::TaskSpawnerUnavailable].
    /// 
    /// [spawn()]: Self::spawn
    /// [spawn_blocking()]: Self::spawn_blocking
    /// [TaskHandle]: crate::TaskHandle
    /// [TaskError::kind()]: crate::TaskError::kind
    /// [TaskErrorKind::TaskSpawnerUnavailable]: crate::TaskErrorKind::TaskSpawnerUnavailable
    /// [TaskErrorKind::PreviousTaskPanic]: crate::TaskErrorKind::PreviousTaskPanic
    pub fn has_panicked(&self) -> Option<bool> {
        self.repr.has_panicked()
    }
}

impl<S: Send + 'static> TaskSpawner<S> {

    /// Queues an asynchronous task for sequential execution
    /// on the associated worker, 
    /// returning a [TaskHandle] to wait for it to complete.
    /// 
    /// This method does not panic or return an error
    /// even when the task should no longer be spawned or cannot be spawned. 
    /// Instead, the returned TaskHandle immediately completes with an error
    /// corresponding to the relevant [TaskErrorKind].
    /// 
    /// [TaskErrorKind]: crate::TaskErrorKind
    /// [TaskHandle]: crate::TaskHandle
    pub fn spawn<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
        R: Send + 'static,
    {
        let (task, task_result, task_canceller) = internal::build_async_task(task);
        match self.repr.send(task) {
            Ok(worker_status) => TaskHandle::new(task_result, task_canceller, worker_status),
            Err(e) => TaskHandle::unspawned(e),
        }
    }

    /// Queues a blocking task for sequential execution
    /// on the associated worker,
    /// returning a [TaskHandle] to wait for it to complete.
    /// 
    /// The blocking task is executed using Tokio's blocking thread pool
    /// to avoid blocking the asynchronous runtime.
    /// 
    /// This method does not panic or return an error
    /// even when the task should no longer be spawned or cannot be spawned. 
    /// Instead, the returned TaskHandle immediately completes with an error
    /// corresponding to the relevant [TaskErrorKind].
    /// 
    /// [TaskErrorKind]: crate::TaskErrorKind
    /// [TaskHandle]: crate::TaskHandle
    pub fn spawn_blocking<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: (FnOnce(&mut S) -> R) + Send + 'static,
        R: Send + 'static,
    {
        let (task, task_result, task_canceller) = internal::build_blocking_task(task);
        match self.repr.send(task) {
            Ok(worker_status) => TaskHandle::new(task_result, task_canceller, worker_status),
            Err(e) => TaskHandle::unspawned(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::{spawn, sync::oneshot, time::sleep};

    #[tokio::test]
    async fn test1() {
        let i = 1000;
        let worker = crate::spawn_worker(0);
        let spawner = worker.spawner();
        
        for _ in 0..i {
            spawner.spawn(|count| Box::pin(async {
                *count += 1;
            }));
        }

        drop(spawner);
        assert_eq!(worker.join().await.unwrap(), i);
    }

    #[tokio::test]
    async fn test2() {
        let worker = crate::spawn_worker(0);
        let spawner = worker.spawner();
        
        spawn(async move {
            sleep(Duration::from_secs(1)).await;
            spawner.spawn(|s| Box::pin(async {
                *s = 1;
            }));
            drop(spawner);
        });

        assert_eq!(worker.join().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test3() {
        let mut worker = crate::spawn_worker(Vec::new());
        let spawner = worker.spawner();
        let (tx, rx) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        
        spawn(async move {
            let h1 = spawner.spawn(|s| Box::pin(async {
                s.push("1");
            }));

            tx.send(()).unwrap();
            rx2.await.unwrap();
            
            let h2 = spawner.spawn(|s| Box::pin(async {
                s.push("2");
            }));

            assert!(h1.await.is_ok());
            assert!(h2.await.unwrap_err().kind().is_task_spawner_unavailable());
        });

        rx.await.unwrap();
        worker.close_spawners();
        tx2.send(()).unwrap();

        let spawner = worker.spawner();
        let h = spawner.spawn(|state| Box::pin(async {
            state.push("3");
        }));

        assert!(h.await.is_ok());

        drop(spawner);
        assert_eq!(worker.join().await.unwrap(), vec!["1", "3"]);
    }

    #[tokio::test]
    async fn test4() {
        let worker = crate::spawn_worker(0);
        let spawner = worker.spawner();
        
        for i in 0..1000 {
            if i % 2 == 0 {
                spawner.spawn(move |count| Box::pin(async move {
                    if i == *count {
                        *count += 1;
                    }
                }));
            }
            else {
                worker.spawn(move |count| Box::pin(async move {
                    if i == *count {
                        *count += 1;
                    }
                }));
            }
        }

        drop(spawner);
        assert_eq!(worker.join().await.unwrap(), 1000);
    }

    #[tokio::test]
    async fn test5() {
        let mut worker = crate::spawn_worker(0);
        let spawner = worker.spawner();

        let h = worker.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.kind().is_task_panic());
        assert!(!err.kind().is_previous_task_panic());

        worker.close_spawners();
        let h = spawner.spawn(|_| Box::pin(async {}));
        let err = h.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
        assert!(err.kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test6() {
        let worker = crate::spawn_worker(0);
        let spawner = worker.spawner();

        let h = worker.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.kind().is_task_panic());
        assert!(!err.kind().is_previous_task_panic());

        worker.abort();
        let h = spawner.spawn(|_| Box::pin(async {}));
        let err = h.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
        assert!(err.kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test7() {
        let worker = crate::spawn_worker(0);
        let spawner = worker.spawner();

        let h = worker.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.kind().is_task_panic());
        assert!(!err.kind().is_previous_task_panic());

        worker.cancel();
        let h = spawner.spawn(|_| Box::pin(async {}));
        let err = h.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
        assert!(err.kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test8() {
        let worker = crate::spawn_worker(0);
        let spawner = worker.spawner();

        let h = worker.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.kind().is_task_panic());
        assert!(!err.kind().is_previous_task_panic());

        spawn(async move {
            sleep(Duration::from_secs(1)).await;
            let h = spawner.spawn(|_| Box::pin(async {}));
            let err = h.await.unwrap_err();
            assert!(!err.is_panic());
            assert!(err.is_cancelled());
            assert!(err.kind().is_task_spawner_unavailable());
        });

        assert!(worker.cancel_and_join().await.is_err());
    }

    #[tokio::test]
    async fn test9() {
        let worker = crate::spawn_worker(0);
        let _spawner = worker.spawner();
        assert!(worker.cancel_and_join().await.is_ok());
    }

    #[tokio::test]
    async fn test10() {
        let mut worker = crate::spawn_worker(0);
        let _spawner = worker.spawner();
        worker.close_spawners();
        assert!(worker.join().await.is_ok());
    }

    #[tokio::test]
    async fn test11() {
        let worker = crate::spawn_worker(0);
        let spawner = worker.spawner();
        worker.abort();
        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test12() {
        let mut worker = crate::spawn_worker(0);
        
        let spawner = worker.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        let h1 = worker.spawn(|_| Box::pin(async { panic!() }));
        let h2 = spawner.spawn(|_| Box::pin(async {}));


        assert!(h1.await.unwrap_err().kind().is_task_panic());
        assert!(h2.await.unwrap_err().kind().is_previous_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        worker.close_spawners();
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test13() {
        let worker = crate::spawn_worker(0);
        
        let spawner = worker.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());

        let h1 = worker.spawn(|_| Box::pin(async { panic!() }));
        let h2 = spawner.spawn(|_| Box::pin(async {}));
        assert!(h1.await.unwrap_err().kind().is_task_panic());
        assert!(h2.await.unwrap_err().kind().is_previous_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        worker.abort();
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test14() {
        let worker = crate::spawn_worker(0);
        
        let spawner = worker.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        worker.spawn(|_| Box::pin(async { panic!() }));

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_previous_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        worker.cancel();
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test15() {
        let worker = crate::spawn_worker(0);
        
        let spawner = worker.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        worker.spawn(|_| Box::pin(async { panic!() }));

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_previous_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        assert!(worker.cancel_and_join().await.is_err());
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test16() {
        let worker = crate::spawn_worker(0);
        
        for _ in 0..1000 {
            let spawner = worker.spawner();
            spawn(async move {
                for _ in 0..1000 {
                    spawner.spawn(|s| Box::pin(async {
                        *s += 1;
                    }));
                }
            });
        }
    
        let result = worker.join().await.unwrap();
        assert_eq!(result, 1000 * 1000);
    }

    #[tokio::test]
    async fn test17() {
        let mut worker = crate::spawn_worker("state");
        let spawner = worker.spawner();
        assert_eq!(spawner.has_panicked(), Some(false));
        assert!(!spawner.is_unavailable());
        worker.close_spawners();
        assert!(spawner.is_unavailable());
        assert_eq!(spawner.has_panicked(), None);
        assert_eq!(worker.join().await.unwrap(), "state");
    }

    #[tokio::test]
    async fn test18() {
        let worker = crate::spawn_worker(());
        let _ = worker.spawn(|_| Box::pin(async { panic!()})).await;

        let spawner = worker.spawner();
        assert_eq!(spawner.has_panicked(), Some(true));
    }

    #[tokio::test]
    async fn test19() {
        let worker = crate::spawn_worker(());

        let spawner = worker.spawner();
        assert_eq!(spawner.has_panicked(), Some(false));
        let _ = spawner.spawn(|_| Box::pin(async { panic!()})).await;
        assert_eq!(spawner.has_panicked(), Some(true));
    }

    #[tokio::test]
    async fn test20() {
        let worker = crate::spawn_worker(());
        let _ = worker.spawn(|_| Box::pin(async { })).await;

        let spawner = worker.spawner();
        assert_eq!(spawner.has_panicked(), Some(false));
        let _ = spawner.spawn(|_| Box::pin(async { panic!()})).await;
        assert_eq!(spawner.has_panicked(), Some(true));
    }
}

#[cfg(test)]
mod asserts {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    fn require_send_static_unpin_unwindsafe<F: Send + 'static + Unpin + UnwindSafe + RefUnwindSafe>(_: F) {}
    fn require_send_static_unpin<F: Send + 'static + Unpin>(_: F) {}

    #[allow(unused)]
    fn assert_impls() {
        let worker = crate::spawn_worker(());
        let spawner = worker.spawner();

        require_send_static_unpin(spawner.spawn(|_| Box::pin(async {})));
        require_send_static_unpin(spawner.spawn_blocking(|_| {}));
        require_send_static_unpin_unwindsafe(spawner);
    }
}