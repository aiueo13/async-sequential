use crate::*;
use std::pin::Pin;


/// A handle for spawning tasks onto a [JoinQueue].
///
/// It can be obtained from [JoinQueue::spawner].
/// 
/// It provides methods equivalent to [JoinQueue::spawn()](JoinQueue::spawn) and [JoinQueue::spawn_blocking()](JoinQueue::spawn_blocking),
/// except that the returned [TaskHandle] immediately returns an error
/// if this TaskSpawner can no longer spawn tasks,
/// such as when the TaskSpawner has been closed
/// or the associated JoinQueue has been aborted or cancelled.
/// 
/// Note that [JoinQueue::join()](JoinQueue::join) and [JoinQueue::try_join()](JoinQueue::try_join) does not complete as long as any TaskSpawner remains alive.
/// To allow it to complete, either drop all TaskSpawners
/// or call [JoinQueue::close_spawners()](JoinQueue::close_spawners) beforehand.
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
    /// or the associated [JoinQueue] has been aborted or cancelled.
    /// From this point onward,
    /// [TaskHandle]s obtained from [spawn()](Self::spawn) or [spawn_blocking()](Self::spawn_blocking)
    /// immediately return errors for which [TaskError::kind()](TaskError::kind) returns [TaskErrorKind::TaskSpawnerUnavailable].
    pub fn is_unavailable(&self) -> bool {
        self.sender.is_unavailable()
    }

    /// Returns true if a task previously executed by the associated [JoinQueue] has panicked.
    /// 
    /// Once a task has panicked, while this TaskSpawner is not unavailable,
    /// [TaskHandle]s obtained from [spawn()](Self::spawn) or [spawn_blocking()](Self::spawn_blocking)
    /// immediately return errors
    /// for which [TaskError::kind()](TaskError::kind) returns [TaskErrorKind::PreviousTaskPanic].
    /// Because the state invariants may have been violated by the task's panic.
    /// 
    /// Returns None if the TaskSpawner is unavailable.
    /// It occurs when the TaskSpawner has been closed
    /// or the JoinQueue has been aborted or cancelled.
    /// From this point onward, 
    /// TaskHandles obtained from spawn or spawn_blocking
    /// immediately return errors for which TaskError::kind() returns [TaskErrorKind::TaskSpawnerUnavailable].
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
    /// This method is equivalent to [JoinQueue::spawn()](JoinQueue::spawn),
    /// except that the returned TaskHandle immediately returns an error
    /// if this TaskSpawner can no longer spawn tasks,
    /// such as when the TaskSpawner has been closed
    /// or the associated [JoinQueue] has been aborted or cancelled.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub fn spawn<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
        R: Send + 'static,
    {
        let (task, task_result, task_canceller) = internal::build_async_task(task);
        match self.sender.send(task) {
            Ok(worker_state) => {
                TaskHandle::new(task_result, task_canceller, worker_state)
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
    /// This method is equivalent to [JoinQueue::spawn_blocking()](JoinQueue::spawn_blocking),
    /// except that the returned TaskHandle immediately returns an error
    /// if this TaskSpawner can no longer spawn tasks,
    /// such as when the TaskSpawner has been closed
    /// or the associated [JoinQueue] has been aborted or cancelled.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub fn spawn_blocking<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: (FnOnce(&mut S) -> R) + Send + 'static,
        R: Send + 'static,
    {
        let (task, task_result, task_canceller) = internal::build_blocking_task(task);
        match self.sender.send(task) {
            Ok(worker_state) => {
                TaskHandle::new(task_result, task_canceller, worker_state)
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
        let queue = JoinQueue::new(0);
        let spawner = queue.spawner();
        
        for _ in 0..i {
            spawner.spawn(|count| Box::pin(async {
                *count += 1;
            }));
        }

        drop(spawner);
        assert_eq!(queue.join().await, i);
    }

    #[tokio::test]
    async fn test2() {
        let queue = JoinQueue::new(0);
        let spawner = queue.spawner();
        
        spawn(async move {
            sleep(Duration::from_secs(1)).await;
            spawner.spawn(|s| Box::pin(async {
                *s = 1;
            }));
            drop(spawner);
        });

        assert_eq!(queue.join().await, 1);
    }

    #[tokio::test]
    async fn test3() {
        let queue = JoinQueue::new(Vec::new());
        let spawner = queue.spawner();
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
        queue.close_spawners();
        tx2.send(()).unwrap();

        let spawner = queue.spawner();
        let h = spawner.spawn(|state| Box::pin(async {
            state.push("3");
        }));

        assert!(h.await.is_ok());

        drop(spawner);
        assert_eq!(queue.join().await, vec!["1", "3"]);
    }

    #[tokio::test]
    async fn test4() {
        let queue = JoinQueue::new(0);
        let spawner = queue.spawner();
        
        for i in 0..1000 {
            if i % 2 == 0 {
                spawner.spawn(move |count| Box::pin(async move {
                    if i == *count {
                        *count += 1;
                    }
                }));
            }
            else {
                queue.spawn(move |count| Box::pin(async move {
                    if i == *count {
                        *count += 1;
                    }
                }));
            }
        }

        drop(spawner);
        assert_eq!(queue.join().await, 1000);
    }

    #[tokio::test]
    async fn test5() {
        let queue = JoinQueue::new(0);
        let spawner = queue.spawner();

        let h = queue.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.kind().is_task_panic());
        assert!(!err.kind().is_previous_task_panic());

        queue.close_spawners();
        let h = spawner.spawn(|_| Box::pin(async {}));
        let err = h.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
        assert!(err.kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test6() {
        let queue = JoinQueue::new(0);
        let spawner = queue.spawner();

        let h = queue.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.kind().is_task_panic());
        assert!(!err.kind().is_previous_task_panic());

        drop(queue);
        let h = spawner.spawn(|_| Box::pin(async {}));
        let err = h.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
        assert!(err.kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test7() {
        let queue = JoinQueue::new(0);
        let spawner = queue.spawner();

        let h = queue.spawn(|_| Box::pin(async { panic!() }));
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.kind().is_task_panic());
        assert!(!err.kind().is_previous_task_panic());

        queue.cancel();
        let h = spawner.spawn(|_| Box::pin(async {}));
        let err = h.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
        assert!(err.kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test8() {
        let queue = JoinQueue::new(0);
        let spawner = queue.spawner();

        let h = queue.spawn(|_| Box::pin(async { panic!() }));
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

        assert!(queue.cancel_and_try_join().await.is_err());
    }

    #[tokio::test]
    async fn test9() {
        let queue = JoinQueue::new(0);
        let _spawner = queue.spawner();
        assert!(queue.cancel_and_try_join().await.is_ok());
    }

    #[tokio::test]
    async fn test10() {
        let queue = JoinQueue::new(0);
        let _spawner = queue.spawner();
        queue.close_spawners();
        assert!(queue.try_join().await.is_ok());
    }

    #[tokio::test]
    async fn test11() {
        let queue = JoinQueue::new(0);
        let spawner = queue.spawner();
        drop(queue);
        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test12() {
        let queue = JoinQueue::new(0);
        
        let spawner = queue.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        queue.spawn(|_| Box::pin(async { panic!() }));

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_previous_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        queue.close_spawners();
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test13() {
        let queue = JoinQueue::new(0);
        
        let spawner = queue.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        queue.spawn(|_| Box::pin(async { panic!() }));

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_previous_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        drop(queue);
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test14() {
        let queue = JoinQueue::new(0);
        
        let spawner = queue.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        queue.spawn(|_| Box::pin(async { panic!() }));

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_previous_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        queue.cancel();
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test15() {
        let queue = JoinQueue::new(0);
        
        let spawner = queue.spawner();

        assert!(!spawner.has_panicked().unwrap());
        assert!(!spawner.is_unavailable());
        queue.spawn(|_| Box::pin(async { panic!() }));

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_previous_task_panic());

        assert!(!spawner.is_unavailable());
        assert!(spawner.has_panicked().unwrap());
        assert!(queue.cancel_and_try_join().await.is_err());
        assert!(spawner.is_unavailable());
        assert!(spawner.has_panicked().is_none());

        let h = spawner.spawn(|_| Box::pin(async {}));
        assert!(h.await.unwrap_err().kind().is_task_spawner_unavailable());
    }

    #[tokio::test]
    async fn test16() {
        let queue = JoinQueue::new(0);
        
        for _ in 0..1000 {
            let spawner = queue.spawner();
            spawn(async move {
                for _ in 0..1000 {
                    spawner.spawn(|s| Box::pin(async {
                        *s += 1;
                    }));
                }
            });
        }
    
        let result = queue.join().await;
        assert_eq!(result, 1000 * 1000);
    }
}

#[cfg(test)]
mod asserts {
    use super::*;

    fn require_send_sync_static<F: Send + Sync + 'static>(_: F) {}

    #[allow(unused)]
    fn assert_impls() {
        let queue = JoinQueue::new(());
        let spawner = queue.spawner();
        require_send_sync_static(spawner.spawn(|_| Box::pin(async {})));
        require_send_sync_static(spawner.spawn_blocking(|_| {}));
        require_send_sync_static(spawner);
    }
}