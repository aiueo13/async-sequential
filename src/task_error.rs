use crate::*;
use std::{any::Any, fmt, sync::Arc};


/// An error that occurred while waiting for a task to complete.
pub struct TaskError {
    repr: Repr,
}

enum Repr {
    QueueAborted,
    QueueCancelled,
    TaskSpawnerUnavailable,
    TaskCancelled,
    TaskPanic {
        panic: internal::PanicPayload
    },
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>
    },
}

impl TaskError {

    pub(crate) fn task_panicked(panic: internal::PanicPayload) -> Self {
        Self { repr: Repr::TaskPanic { panic } }
    }

    pub(crate) fn prev_task_panicked(panic_msg: Option<Arc<String>>) -> Self {
        Self { repr: Repr::PrevTaskPanic { panic_msg } }
    }

    pub(crate) fn worker_aborted() -> Self {
        Self { repr: Repr::QueueAborted }
    }

    pub(crate) fn worker_cancelled() -> Self {
        Self { repr: Repr::QueueCancelled }
    }

    pub(crate) fn worker_task_sender_unavailable() -> Self {
        Self { repr: Repr::TaskSpawnerUnavailable }
    }

    pub(crate) fn task_cancelled() -> Self {
        Self { repr: Repr::TaskCancelled }
    }

    pub(crate) fn panic(self) -> ! {
        match self.repr {
            Repr::QueueAborted => panic!("queue was aborted"),
            Repr::QueueCancelled => panic!("queue was cancelled"),
            Repr::TaskSpawnerUnavailable => panic!("task spawner was unavailable"),
            Repr::TaskCancelled => panic!("task was cancelled"),
            Repr::TaskPanic { panic } => panic.resume_unwind(),
            Repr::PrevTaskPanic { panic_msg } => {
                match panic_msg {
                    Some(panic_msg) => panic!("previous task panicked: {panic_msg}"),
                    None => panic!("previous task panicked"),
                }
            },
        }
    }
}

impl TaskError {

    /// Extracts the original panic payload
    /// if the error was caused by the task itself panicking.
    ///
    /// The returned panic payload can be passed to
    /// [std::panic::resume_unwind] to resume the original panic.
    ///
    /// # Panics
    /// Panics if the error was not caused by the task itself panicking.
    /// In particular, the panic payload cannot be retrieved
    /// if the error was caused by a previous task panicking.
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        match self.repr {
            Repr::QueueAborted => panic!("cannot extract panic payload: queue was aborted"),
            Repr::QueueCancelled => panic!("cannot extract panic payload: queue was cancelled"),
            Repr::TaskSpawnerUnavailable => panic!("cannot extract panic payload: task spawner was unavailable"),
            Repr::TaskCancelled => panic!("cannot extract panic payload: task was cancelled"),
            Repr::TaskPanic { panic } => panic.into_inner(),
            Repr::PrevTaskPanic { panic_msg } => {
                match panic_msg {
                    Some(panic_msg) => panic!("cannot extract panic payload: a previous task panicked: {panic_msg}"),
                    None => panic!("cannot extract panic payload: a previous task panicked"),
                }
            },
        }
    }

    /// Attempts to extract the original panic payload
    /// if the error was caused by the task itself panicking.
    ///
    /// The returned panic payload can be passed to
    /// [std::panic::resume_unwind] to resume the original panic.
    ///
    /// # Errors
    /// Returns the original error if it was not caused by the task itself panicking.
    /// In particular, the panic payload cannot be retrieved
    /// if the error was caused by a previous task panicking.
    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, Self> {
        match self.repr {
            Repr::QueueAborted => Err(self),
            Repr::QueueCancelled => Err(self),
            Repr::TaskSpawnerUnavailable => Err(self),
            Repr::TaskCancelled => Err(self),
            Repr::TaskPanic { panic } => Ok(panic.into_inner()),
            Repr::PrevTaskPanic { .. } => Err(self),
        }
    }

    /// Returns true if the error was caused by the task was cancelled, either explicitly or implicitly.
    /// 
    /// It occurs when any of the following are true:
    /// - [is_queue_aborted](Self::is_queue_aborted)
    /// - [is_queue_cancelled](Self::is_queue_cancelled)
    /// - [is_task_spawner_unavailable](Self::is_task_spawner_unavailable)
    /// - [is_task_cancelled](Self::is_task_cancelled)
    pub fn is_cancelled(&self) -> bool {
        self.is_queue_aborted() ||
        self.is_queue_cancelled() ||
        self.is_task_spawner_unavailable() || 
        self.is_task_cancelled() 
    }

    /// Returns true if the error was caused by the task or any previous task panicking.
    /// 
    /// It occurs when any of the following are true:
    /// - [is_task_panic](Self::is_task_panic)
    /// - [is_prev_task_panic](Self::is_prev_task_panic)
    pub fn is_panic(&self) -> bool {
        self.is_task_panic() ||
        self.is_prev_task_panic()
    }

    /// Returns true if the error was caused by the task panicking.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let queue = async_sequential::JoinQueue::new(());
    /// 
    /// // Task panic
    /// let handle = queue.spawn(move |_| Box::pin(async move {
    ///     panic!()
    /// }));
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_panic());
    /// assert!(err.is_task_panic());
    /// assert!(!err.is_prev_task_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_task_panic(&self) -> bool {
        matches!(&self.repr, Repr::TaskPanic { .. })
    }

    /// Returns true if the error was caused by any previous task panicking.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let queue = async_sequential::JoinQueue::new(());
    /// 
    /// // Task panic
    /// let handle = queue.spawn(move |_| Box::pin(async move {
    ///     panic!()
    /// }));
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_panic());
    /// assert!(err.is_task_panic());
    /// assert!(!err.is_prev_task_panic());
    /// 
    /// // Subsequent tasks also panic
    /// // because the state invariants may have been violated
    /// // by the preceding task's panic.
    /// let handle = queue.spawn(move |_| Box::pin(async move {
    ///     unreachable!();
    /// }));
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_panic());
    /// assert!(err.is_prev_task_panic());
    /// assert!(!err.is_task_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_prev_task_panic(&self) -> bool {
        matches!(&self.repr, Repr::PrevTaskPanic { .. })
    }

    /// Returns true if the error was caused by the [JoinQueue](crate::JoinQueue) being aborted.
    /// 
    /// It occurs when the task is aborted as a result of the JoinQueue being dropped.  
    /// 
    /// Note that blocking tasks are not asynchronous, so if one is already running,
    /// aborting it only detaches the task from the JoinQueue;
    /// it continues running normally.
    /// In this case, its [TaskHandle](crate::TaskHandle) does not return this error.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let queue = async_sequential::JoinQueue::new(());
    /// 
    /// let running = queue.spawn(move |_| Box::pin(async move {
    ///     // Never completes
    ///     std::future::pending::<()>().await;
    /// }));
    /// drop(queue);
    /// 
    /// // The task was aborted 
    /// // because the JoinQueue was dropped before the task completed.
    /// let err = running.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_queue_aborted());
    /// # assert!(!err.is_task_spawner_unavailable());
    /// # assert!(!err.is_queue_cancelled());
    /// # assert!(!err.is_task_cancelled());
    /// # assert!(!err.is_panic());
    /// # });
    /// # }
    /// ```
    /// 
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use std::{time::Duration, thread};
    /// use tokio::time::sleep;
    /// 
    /// let queue = async_sequential::JoinQueue::new(());
    /// 
    /// let running = queue.spawn_blocking(move |_| {
    ///     thread::sleep(Duration::from_secs(2));
    ///     "complete"
    /// });
    /// let queued = queue.spawn_blocking(move |_| {
    ///     unreachable!();
    /// });
    /// 
    /// sleep(Duration::from_secs(1)).await;
    /// drop(queue);
    /// 
    /// // The task was not aborted 
    /// // because the blocking task was started.
    /// assert_eq!(running.await.unwrap(), "complete");
    /// 
    /// // The task was aborted 
    /// // because the blocking task was not started.
    /// let err = queued.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_queue_aborted());
    /// # assert!(!err.is_task_spawner_unavailable());
    /// # assert!(!err.is_queue_cancelled());
    /// # assert!(!err.is_task_cancelled());
    /// # assert!(!err.is_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_queue_aborted(&self) -> bool {
        matches!(&self.repr, Repr::QueueAborted)
    }

    /// Returns true if the error was caused by the [JoinQueue](crate::JoinQueue) being cancelled.
    /// 
    /// It occurs when the task is cancelled as a result of [JoinQueue::cancel](crate::JoinQueue::cancel) being called.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let queue = async_sequential::JoinQueue::new(());
    /// 
    /// let _running = queue.spawn(move |_| Box::pin(async move {
    ///     // Never completes
    ///     std::future::pending::<()>().await;
    /// }));
    /// 
    /// let queued = queue.spawn(move |_| Box::pin(async move {
    ///     unreachable!();
    /// }));
    /// queue.cancel();
    /// # // JoinQueue キャンセル後にタスクをキャンセルしても
    /// # // err.is_task_cancelled() は true にならないべき
    /// # queued.cancel();
    /// 
    /// // The task was cancelled 
    /// // by its handle before it started.
    /// let err = queued.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_queue_cancelled());
    /// # assert!(!err.is_queue_aborted());
    /// # assert!(!err.is_task_spawner_unavailable());
    /// # assert!(!err.is_task_cancelled());
    /// # assert!(!err.is_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_queue_cancelled(&self) -> bool {
        matches!(&self.repr, Repr::QueueCancelled)
    }

    /// Returns true if the error was caused by the task being spawned
    /// after the [TaskSpawner] could no longer spawn tasks.
    /// 
    /// It occurs when the task is cancelled before the task is spawned by the TaskSpawner as a result of any of the following:
    /// - [JoinQueue] being aborted before spawning tasks.
    /// - JoinQueue being cancelled before spawning tasks.
    /// - TaskSpawner being closed before spawning tasks.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use tokio::{task::spawn, time::sleep};
    /// use std::time::Duration;
    /// 
    /// let queue = async_sequential::JoinQueue::new(());
    /// 
    /// let _ = queue.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    /// }));
    /// 
    /// let spawner = queue.spawner();
    /// 
    /// let task = spawn(async move {
    ///     sleep(Duration::from_secs(1)).await;
    ///     spawner.spawn(move |_| Box::pin(async move {
    ///         unreachable!();
    ///     }))
    /// });
    /// 
    /// queue.close_spawners();
    /// queue.join().await;
    /// 
    /// // The task was cancelled 
    /// // because the spawner was closed before the task was spawned.
    /// let err = task.await.unwrap().await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_task_spawner_unavailable());
    /// # assert!(!err.is_queue_aborted());
    /// # assert!(!err.is_queue_cancelled());
    /// # assert!(!err.is_task_cancelled());
    /// # assert!(!err.is_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_task_spawner_unavailable(&self) -> bool {
        matches!(&self.repr, Repr::TaskSpawnerUnavailable)
    }

    /// Returns true if the error was caused by the task being cancelled through its handle.
    /// 
    /// It occurs when the task is cancelled as a result of any of the following methods being called.
    /// - [TaskHandle::cancel](crate::TaskHandle::cancel)
    /// - [TaskCanceller::cancel](crate::TaskCanceller::cancel)
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let queue = async_sequential::JoinQueue::new(());
    /// 
    /// let _running = queue.spawn(move |_| Box::pin(async move {
    ///     // Never completes
    ///     std::future::pending::<()>().await;
    /// }));
    /// 
    /// let queued = queue.spawn(move |_| Box::pin(async move {
    ///     unreachable!();
    /// }));
    /// queued.cancel();
    /// # // タスクキャンセル後に JoinQueue　をキャンセルしても
    /// # // err.is_queue_cancelled() は true にならないべき
    /// # queue.cancel();
    /// 
    /// // The task was cancelled 
    /// // by its handle before it started.
    /// let err = queued.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_task_cancelled());
    /// # assert!(!err.is_queue_aborted());
    /// # assert!(!err.is_task_spawner_unavailable());
    /// # assert!(!err.is_queue_cancelled());
    /// # assert!(!err.is_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_task_cancelled(&self) -> bool {
        matches!(&self.repr, Repr::TaskCancelled)
    }
}

impl fmt::Debug for TaskError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::QueueAborted => f.write_str("TaskError::QueueAborted"),
            Repr::QueueCancelled => f.write_str("TaskError::QueueCancelled"),
            Repr::TaskSpawnerUnavailable => f.write_str("TaskError::TaskSpawnerUnavailable"),
            Repr::TaskCancelled => f.write_str("TaskError::TaskCancelled"),
            Repr::PrevTaskPanic { panic_msg } => {
                f.debug_struct("TaskError::PreviousTaskPanic")
                    .field("panic_msg", panic_msg)
                    .finish()
            }
            Repr::TaskPanic { panic } => {
                f.debug_struct("TaskError::TaskPanic")
                    .field("panic_msg", &panic.msg())
                    .finish()
            }
        }
    }
}

impl fmt::Display for TaskError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::QueueAborted => f.write_str("queue was aborted"),
            Repr::QueueCancelled => f.write_str("queue was cancelled"),
            Repr::TaskSpawnerUnavailable => f.write_str("task spawner was unavailable"),
            Repr::TaskCancelled => f.write_str("task was cancelled"),
            Repr::PrevTaskPanic { panic_msg } => {
                match panic_msg {
                    Some(msg) => write!(f, "previous task panicked: {msg}"),
                    None => f.write_str("previous task panicked"),
                }
            }
            Repr::TaskPanic { panic } => {
                match panic.msg() {
                    Some(msg) => write!(f, "task panicked: {msg}"),
                    None => f.write_str("task panicked"),
                }
            }
        }
    }
}

impl std::error::Error for TaskError {}

impl From<TaskError> for std::io::Error {

    fn from(value: TaskError) -> std::io::Error {
        std::io::Error::other(value)
    }
}