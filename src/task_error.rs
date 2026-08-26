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
    PreviousTaskPanic {
        panic_msg: Option<Arc<String>>
    },
}

impl TaskError {

    pub(crate) fn task_panicked(panic: internal::PanicPayload) -> Self {
        Self { repr: Repr::TaskPanic { panic } }
    }

    pub(crate) fn prev_task_panicked(panic_msg: Option<Arc<String>>) -> Self {
        Self { repr: Repr::PreviousTaskPanic { panic_msg } }
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
            Repr::PreviousTaskPanic { panic_msg } => {
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
    /// [std::panic::resume_unwind()](std::panic::resume_unwind) to resume the original panic.
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
            Repr::PreviousTaskPanic { panic_msg } => {
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
    /// [std::panic::resume_unwind()](std::panic::resume_unwind) to resume the original panic.
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
            Repr::PreviousTaskPanic { .. } => Err(self),
        }
    }

    /// Returns true if the error was caused by the task being cancelled,
    /// either explicitly or implicitly.
    ///
    /// This occurs when [kind()](Self::kind) returns any of the following.
    /// - [TaskErrorKind::QueueAborted]
    /// - [TaskErrorKind::QueueCancelled]
    /// - [TaskErrorKind::TaskSpawnerUnavailable]
    /// - [TaskErrorKind::TaskCancelled]
    pub fn is_cancelled(&self) -> bool {
        let kind = self.kind();
        kind.is_queue_aborted() ||
        kind.is_queue_cancelled() ||
        kind.is_task_spawner_unavailable() ||
        kind.is_task_cancelled()
    }

    /// Returns true if the error was caused by the task or any previous task panicking.
    ///
    /// This occurs when [kind()](Self::kind) returns any of the following.
    /// - [TaskErrorKind::TaskPanic]
    /// - [TaskErrorKind::PreviousTaskPanic]
    pub fn is_panic(&self) -> bool {
        let kind = self.kind();
        kind.is_task_panic() || kind.is_previous_task_panic()
    }

    /// Returns the [TaskErrorKind] corresponding to this error.
    pub fn kind(&self) -> TaskErrorKind {
        match self.repr {
            Repr::QueueAborted => TaskErrorKind::QueueAborted,
            Repr::QueueCancelled => TaskErrorKind::QueueCancelled,
            Repr::TaskSpawnerUnavailable => TaskErrorKind::TaskSpawnerUnavailable,
            Repr::TaskCancelled => TaskErrorKind::TaskCancelled,
            Repr::TaskPanic { .. } => TaskErrorKind::TaskPanic,
            Repr::PreviousTaskPanic { .. } => TaskErrorKind::PreviousTaskPanic,
        }
    }
}

impl fmt::Debug for TaskError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::QueueAborted => f.write_str("TaskError::QueueAborted"),
            Repr::QueueCancelled => f.write_str("TaskError::QueueCancelled"),
            Repr::TaskSpawnerUnavailable => f.write_str("TaskError::TaskSpawnerUnavailable"),
            Repr::TaskCancelled => f.write_str("TaskError::TaskCancelled"),
            Repr::PreviousTaskPanic { panic_msg } => {
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
            Repr::PreviousTaskPanic { panic_msg } => {
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


/// The kind of error that occurred while waiting for a task to complete.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum TaskErrorKind {

    /// The error was caused by the [TaskQueue](crate::TaskQueue) being aborted.
    ///
    /// See [is_queue_aborted()](Self::is_queue_aborted) for details.
    QueueAborted,

    /// The error was caused by the [TaskQueue](crate::TaskQueue) being cancelled.
    ///
    /// See [is_queue_cancelled()](Self::is_queue_cancelled) for details.
    QueueCancelled,

    /// The error was caused by the task being spawned
    /// after the [TaskSpawner](crate::TaskSpawner) could no longer spawn tasks.
    ///
    /// See [is_task_spawner_unavailable()](Self::is_task_spawner_unavailable) for details.
    TaskSpawnerUnavailable,

    /// The error was caused by the task being cancelled through its handle.
    ///
    /// See [is_task_cancelled()](Self::is_task_cancelled) for details.
    TaskCancelled,

    /// The error was caused by the task panicking.
    ///
    /// See [is_task_panic()](Self::is_task_panic) for details.
    TaskPanic,

    /// The error was caused by any previous task panicking.
    ///
    /// See [is_previous_task_panic()](Self::is_previous_task_panic) for details.
    PreviousTaskPanic
}

impl TaskErrorKind {

    /// Returns true if the error was caused by the [TaskQueue](crate::TaskQueue) being aborted.
    ///
    /// Note that blocking tasks are not asynchronous, so if one is already running,
    /// aborting it only detaches the task from the TaskQueue;
    /// it continues running normally.
    /// In this case, its [TaskHandle](crate::TaskHandle) does not return this error.
    ///
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let queue = async_sequential::TaskQueue::new(());
    ///
    /// let running = queue.spawn(move |_| Box::pin(async move {
    ///     // Never completes
    ///     std::future::pending::<()>().await;
    /// }));
    /// drop(queue);
    ///
    /// // The task was aborted
    /// // because the TaskQueue was dropped before the task completed.
    /// let err = running.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.kind().is_queue_aborted());
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
    /// let queue = async_sequential::TaskQueue::new(());
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
    /// assert!(err.kind().is_queue_aborted());
    /// # assert!(!err.kind().is_task_spawner_unavailable());
    /// # assert!(!err.kind().is_queue_cancelled());
    /// # assert!(!err.kind().is_task_cancelled());
    /// # assert!(!err.is_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_queue_aborted(&self) -> bool {
        matches!(self, TaskErrorKind::QueueAborted)
    }

    /// Returns true if the error was caused by the [TaskQueue](crate::TaskQueue) being cancelled.
    ///
    /// This occurs when the task is cancelled as a result of [TaskQueue::cancel()](crate::TaskQueue::cancel) being called.
    ///
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let queue = async_sequential::TaskQueue::new(());
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
    ///
    /// // The task was cancelled
    /// // by the TaskQueue before it started.
    /// let err = queued.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.kind().is_queue_cancelled());
    /// # });
    /// # }
    /// ```
    pub fn is_queue_cancelled(&self) -> bool {
        matches!(self, TaskErrorKind::QueueCancelled)
    }

    /// Returns true if the error was caused by the task being spawned
    /// after the [TaskSpawner](crate::TaskSpawner) could no longer spawn tasks.
    ///
    /// This occurs when the task is cancelled before the task is spawned by the TaskSpawner as a result of any of the following:
    /// - [TaskQueue](crate::TaskQueue) being aborted before spawning tasks.
    /// - TaskQueue being cancelled before spawning tasks.
    /// - TaskSpawner being closed before spawning tasks.
    ///
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use tokio::{task::spawn, time::sleep};
    /// use std::time::Duration;
    ///
    /// let queue = async_sequential::TaskQueue::new(());
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
    /// assert!(err.kind().is_task_spawner_unavailable());
    /// # });
    /// # }
    /// ```
    pub fn is_task_spawner_unavailable(&self) -> bool {
        matches!(self, TaskErrorKind::TaskSpawnerUnavailable)
    }

    /// Returns true if the error was caused by the task being cancelled through its handle.
    ///
    /// This occurs when the task is cancelled as a result of any of the following methods being called.
    /// - [TaskHandle::cancel()](crate::TaskHandle::cancel)
    /// - [TaskCanceller::cancel()](crate::TaskCanceller::cancel)
    ///
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let queue = async_sequential::TaskQueue::new(());
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
    ///
    /// // The task was cancelled
    /// // by its handle before it started.
    /// let err = queued.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.kind().is_task_cancelled());
    /// # });
    /// # }
    /// ```
    pub fn is_task_cancelled(&self) -> bool {
        matches!(self, TaskErrorKind::TaskCancelled)
    }

    /// Returns true if the error was caused by the task panicking.
    ///
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let queue = async_sequential::TaskQueue::new(());
    ///
    /// // Task panic
    /// let handle = queue.spawn(move |_| Box::pin(async move {
    ///     panic!()
    /// }));
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_panic());
    /// assert!(err.kind().is_task_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_task_panic(&self) -> bool {
        matches!(self, TaskErrorKind::TaskPanic)
    }

    /// Returns true if the error was caused by any previous task panicking.
    ///
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let queue = async_sequential::TaskQueue::new(());
    ///
    /// // Task panic
    /// let handle = queue.spawn(move |_| Box::pin(async move {
    ///     panic!()
    /// }));
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_panic());
    /// assert!(err.kind().is_task_panic());
    ///
    /// // Subsequent tasks also panic
    /// // because the state invariants may have been violated
    /// // by the preceding task's panic.
    /// let handle = queue.spawn(move |_| Box::pin(async move {
    ///     unreachable!();
    /// }));
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_panic());
    /// assert!(err.kind().is_previous_task_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_previous_task_panic(&self) -> bool {
        matches!(self, TaskErrorKind::PreviousTaskPanic)
    }
}

impl fmt::Display for TaskErrorKind {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskErrorKind::QueueAborted => f.write_str("queue was aborted"),
            TaskErrorKind::QueueCancelled => f.write_str("queue was cancelled"),
            TaskErrorKind::TaskSpawnerUnavailable => f.write_str("task spawner was unavailable"),
            TaskErrorKind::TaskCancelled => f.write_str("task was cancelled"),
            TaskErrorKind::PreviousTaskPanic => f.write_str("previous task panicked"),
            TaskErrorKind::TaskPanic => f.write_str("task panicked"),
        }
    }
}