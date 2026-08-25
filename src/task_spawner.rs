use crate::*;
use std::pin::Pin;


/// Handle for spawning tasks onto an executor.
///
/// It can be obtained from [Executor::spawner].
/// 
/// It provides methods equivalent to [Executor::spawn] and [Executor::spawn_blocking],
/// except that the returned [TaskHandle] immediately returns an error
/// if this TaskSpawner can no longer spawn tasks,
/// such as when the TaskSpawner has been closed
/// or the associated [Executor] has been aborted or cancelled.
/// In such cases, [TaskError::is_task_spawner_unavailable] returns true.
/// 
/// Note that [Executor::join] and [Executor::try_join] does not complete as long as any TaskSpawner remains alive.
/// To allow it to complete, either drop all TaskSpawners
/// or call [Executor::close_spawners] beforehand.
pub struct TaskSpawner<S> {
    sender: internal::WorkerTaskSender<S>,
}

impl<S> TaskSpawner<S> {

    pub(crate) fn new(sender: internal::WorkerTaskSender<S>) -> Self {
        Self { sender }
    }
}

impl<S> TaskSpawner<S> {

    /// Returns true if this TaskSpawner can no longer spawn tasks.
    ///
    /// It occurs when the TaskSpawner has been closed
    /// or the associated [Executor] has been aborted or cancelled.
    /// From this point onward,
    /// [TaskHandle]s obtained from [spawn](Self::spawn) or [spawn_blocking](Self::spawn_blocking)
    /// immediately return errors for which [TaskError::is_task_spawner_unavailable] returns true.
    pub fn is_unavailable(&self) -> bool {
        self.sender.is_unavailable()
    }

    /// Returns true if a task previously executed by the associated [Executor] has panicked.
    /// 
    /// Once a task has panicked, while this TaskSpawner is not unavailable,
    /// [TaskHandle]s obtained from [spawn](Self::spawn) or [spawn_blocking](Self::spawn_blocking)
    /// immediately return errors for which [TaskError::is_prev_task_panic] returns true.
    /// Because the state invariants may have been violated by the task's panic.
    /// 
    /// Returns None if the TaskSpawner is unavailable.
    /// It occurs when the TaskSpawner has been closed
    /// or the [Executor] has been aborted or cancelled.
    /// From this point onward, 
    /// [TaskHandle]s obtained from spawn or spawn_blocking
    /// immediately return errors for which [TaskError::is_task_spawner_unavailable] returns true.
    pub fn has_panicked(&self) -> Option<bool> {
        self.sender.has_panicked()
    }
}

impl<S: Send + 'static> TaskSpawner<S> {

    /// Queues an asynchronous task for sequential execution, 
    /// returning a [TaskHandle] to wait for it to complete.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    ///
    /// This method is equivalent to [Executor::spawn],
    /// except that the returned TaskHandle immediately returns an error
    /// if this TaskSpawner can no longer spawn tasks,
    /// such as when the TaskSpawner has been closed
    /// or the associated [Executor] has been aborted or cancelled.
    /// In such cases, [TaskError::is_task_spawner_unavailable] returns true.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub fn spawn<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
        R: Send + 'static,
    {
        let (task, task_result, task_controller) = internal::build_async_task(task);
        match self.sender.send(task) {
            Ok(worker_state) => {
                TaskHandle::new(task_result, task_controller, worker_state)
            },
            Err(internal::WorkerTaskSenderSendError::PrevTaskPanic { panic_msg }) => {
                TaskHandle::prev_task_panicked(panic_msg)
            },
            Err(internal::WorkerTaskSenderSendError::Unavailable) => {
                TaskHandle::worker_task_sender_unavailable()
            } 
        }
    }

    /// Queues a blocking task for sequential execution, 
    /// returning a [TaskHandle] to wait for it to complete.
    ///
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    /// 
    /// The blocking task is executed using [Tokio's blocking thread pool](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html)
    /// to avoid blocking the asynchronous runtime.
    /// 
    /// This method is equivalent to [Executor::spawn_blocking],
    /// except that the returned TaskHandle immediately returns an error
    /// if this TaskSpawner can no longer spawn tasks,
    /// such as when the TaskSpawner has been closed
    /// or the associated [Executor] has been aborted or cancelled.
    /// In such cases, [TaskError::is_task_spawner_unavailable] returns true.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub fn spawn_blocking<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: (FnOnce(&mut S) -> R) + Send + 'static,
        R: Send + 'static,
    {
        let (task, task_result, task_controller) = internal::build_blocking_task(task);
        match self.sender.send(task) {
            Ok(worker_state) => {
                TaskHandle::new(task_result, task_controller, worker_state)
            },
            Err(internal::WorkerTaskSenderSendError::PrevTaskPanic { panic_msg }) => {
                TaskHandle::prev_task_panicked(panic_msg)
            },
            Err(internal::WorkerTaskSenderSendError::Unavailable) => {
                TaskHandle::worker_task_sender_unavailable()
            } 
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::{spawn, sync::oneshot, time::sleep};
    use super::*;

    #[tokio::test]
    async fn test1() {
        let i = 1000;
        let executor = Executor::new(0);
        let spawner = executor.spawner();
        
        for _ in 0..i {
            spawner.spawn(|count| Box::pin(async {
                *count += 1;
            }));
        }

        drop(spawner);
        assert_eq!(executor.join().await, i);
    }

    #[tokio::test]
    async fn test2() {
        let executor = Executor::new(0);
        let spawner = executor.spawner();
        
        spawn(async move {
            sleep(Duration::from_secs(1)).await;
            spawner.spawn(|s| Box::pin(async {
                *s = 1;
            }));
            drop(spawner);
        });

        assert_eq!(executor.join().await, 1);
    }

    #[tokio::test]
    async fn test3() {
        let executor = Executor::new(Vec::new());
        let spawner = executor.spawner();
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
            assert!(h2.await.unwrap_err().is_task_spawner_unavailable());
        });

        rx.await.unwrap();
        executor.close_spawners();
        tx2.send(()).unwrap();

        let spawner = executor.spawner();
        let h = spawner.spawn(|state| Box::pin(async {
            state.push("3");
        }));

        assert!(h.await.is_ok());

        drop(spawner);
        assert_eq!(executor.join().await, vec!["1", "3"]);
    }

    #[tokio::test]
    async fn test4() {
        let executor = Executor::new(0);
        let spawner = executor.spawner();
        
        for i in 0..1000 {
            if i % 2 == 0 {
                spawner.spawn(move |count| Box::pin(async move {
                    if i == *count {
                        *count += 1;
                    }
                }));
            }
            else {
                executor.spawn(move |count| Box::pin(async move {
                    if i == *count {
                        *count += 1;
                    }
                }));
            }
        }

        drop(spawner);
        assert_eq!(executor.join().await, 1000);
    }

    #[tokio::test]
    async fn test5() {
        let executor = Executor::new(0);
        let spawner = executor.spawner();

        let h = executor.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.is_task_panic());
        assert!(!err.is_prev_task_panic());

        executor.close_spawners();
        let h = spawner.spawn(|_| Box::pin(async {}));
        let err = h.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
        assert!(err.is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test6() {
        let executor = Executor::new(0);
        let spawner = executor.spawner();

        let h = executor.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.is_task_panic());
        assert!(!err.is_prev_task_panic());

        drop(executor);
        let h = spawner.spawn(|_| Box::pin(async {}));
        let err = h.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
        assert!(err.is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test7() {
        let executor = Executor::new(0);
        let spawner = executor.spawner();

        let h = executor.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.is_task_panic());
        assert!(!err.is_prev_task_panic());

        executor.cancel();
        let h = spawner.spawn(|_| Box::pin(async {}));
        let err = h.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
        assert!(err.is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test8() {
        let executor = Executor::new(0);
        let spawner = executor.spawner();

        let h = executor.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.is_task_panic());
        assert!(!err.is_prev_task_panic());

        spawn(async move {
            sleep(Duration::from_secs(1)).await;
            let h = spawner.spawn(|_| Box::pin(async {}));
            let err = h.await.unwrap_err();
            assert!(!err.is_panic());
            assert!(err.is_cancelled());
            assert!(err.is_task_spawner_unavailable());
        });

        assert!(executor.cancel_and_try_join().await.is_err());
    }

    #[tokio::test]
    async fn test9() {
        let executor = Executor::new(0);
        let _spawner = executor.spawner();
        assert!(executor.cancel_and_try_join().await.is_ok());
    }

    #[tokio::test]
    async fn test10() {
        let executor = Executor::new(0);
        let _spawner = executor.spawner();
        executor.close_spawners();
        assert!(executor.try_join().await.is_ok());
    }

    #[tokio::test]
    async fn test11() {
        let executor = Executor::new(0);
        let spawner = executor.spawner();
        drop(executor);
        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test12() {
        let executor = Executor::new(0);
        
        let spawner = executor.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        executor.spawn(|_| Box::pin(async { panic!() }));

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().is_prev_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        executor.close_spawners();
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test13() {
        let executor = Executor::new(0);
        
        let spawner = executor.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        executor.spawn(|_| Box::pin(async { panic!() }));

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().is_prev_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        drop(executor);
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test14() {
        let executor = Executor::new(0);
        
        let spawner = executor.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        executor.spawn(|_| Box::pin(async { panic!() }));

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().is_prev_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        executor.cancel();
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test15() {
        let executor = Executor::new(0);
        
        let spawner = executor.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        executor.spawn(|_| Box::pin(async { panic!() }));

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().is_prev_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        assert!(executor.cancel_and_try_join().await.is_err());
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test16() {
        let executor = Executor::new(0);
        
        for _ in 0..1000 {
            let spawner = executor.spawner();
            spawn(async move {
                for _ in 0..1000 {
                    spawner.spawn(|s| Box::pin(async {
                        *s += 1;
                    }));
                }
            });
        }
    
        let result = executor.join().await;
        assert_eq!(result, 1000 * 1000);
    }
}

#[cfg(test)]
mod asserts {
    use super::*;

    fn require_send_sync_static<F: Send + Sync + 'static>(_: F) {}

    #[allow(unused)]
    fn assert_impls() {
        let executor = Executor::new(());
        let spawner = executor.spawner();
        require_send_sync_static(spawner.spawn(|_| Box::pin(async {})));
        require_send_sync_static(spawner.spawn_blocking(|_| {}));
        require_send_sync_static(spawner);
    }
}