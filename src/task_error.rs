use crate::*;
use std::{any::Any, fmt, sync::Arc};


/// An error that occurred while waiting for a task to complete.
pub struct TaskError {
    repr: Repr,
}

enum Repr {
    WorkerAborted,
    WorkerCancelled,
    TaskSpawnerUnavailable,
    TaskCancelled,
    TaskPanic {
        panic: internal::PanicPayload
    },
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>
    },
    RuntimeShutdown,
}

impl TaskError {

    pub(crate) fn task_panicked(panic: internal::PanicPayload) -> Self {
        Self { repr: Repr::TaskPanic { panic } }
    }

    pub(crate) fn prev_task_panicked(panic_msg: Option<Arc<String>>) -> Self {
        Self { repr: Repr::PrevTaskPanic { panic_msg } }
    }

    pub(crate) fn worker_aborted() -> Self {
        Self { repr: Repr::WorkerAborted }
    }

    pub(crate) fn worker_cancelled() -> Self {
        Self { repr: Repr::WorkerCancelled }
    }
    
    pub(crate) fn runtime_shutdown() -> Self {
        Self { repr: Repr::RuntimeShutdown }
    }

    pub(crate) fn worker_task_sender_unavailable() -> Self {
        Self { repr: Repr::TaskSpawnerUnavailable }
    }

    pub(crate) fn task_cancelled() -> Self {
        Self { repr: Repr::TaskCancelled }
    }
}

impl TaskError {

    /// Extracts the original panic payload
    /// if the error was caused by the task itself panicking.
    ///
    /// The returned panic payload can be passed to
    /// [std::panic::resume_unwind()] to resume the original panic.
    ///
    /// # Panics
    /// Panics if the error was not caused by the task itself panicking.
    /// 
    /// [std::panic::resume_unwind()]: std::panic::resume_unwind
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        match self.repr {
            Repr::WorkerAborted => panic!("cannot extract panic payload: worker was aborted"),
            Repr::WorkerCancelled => panic!("cannot extract panic payload: worker was cancelled"),
            Repr::TaskSpawnerUnavailable => panic!("cannot extract panic payload: task spawner was unavailable"),
            Repr::TaskCancelled => panic!("cannot extract panic payload: task was cancelled"),
            Repr::TaskPanic { panic } => panic.into_inner(),
            Repr::PrevTaskPanic { panic_msg } => {
                match panic_msg {
                    Some(panic_msg) => panic!("cannot extract panic payload: previous task panicked: {panic_msg}"),
                    None => panic!("cannot extract panic payload: previous task panicked"),
                }
            },
            Repr::RuntimeShutdown => panic!("cannot extract panic payload: the Tokio runtime was shut down")
        }
    }

    /// Attempts to extract the original panic payload
    /// if the error was caused by the task itself panicking.
    ///
    /// The returned panic payload can be passed to
    /// [std::panic::resume_unwind()] to resume the original panic.
    ///
    /// # Errors
    /// Returns the original error if it was not caused by the task itself panicking.
    /// 
    /// [std::panic::resume_unwind()]: std::panic::resume_unwind
    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, Self> {
        match self.repr {
            Repr::WorkerAborted => Err(self),
            Repr::WorkerCancelled => Err(self),
            Repr::TaskSpawnerUnavailable => Err(self),
            Repr::TaskCancelled => Err(self),
            Repr::TaskPanic { panic } => Ok(panic.into_inner()),
            Repr::PrevTaskPanic { .. } => Err(self),
            Repr::RuntimeShutdown => Err(self)
        }
    }

    /// Returns true if the error was caused by the task being cancelled,
    /// either explicitly or implicitly.
    ///
    /// This occurs when [kind()] returns any of the following.
    /// - [TaskErrorKind::WorkerAborted]
    /// - [TaskErrorKind::WorkerCancelled]
    /// - [TaskErrorKind::TaskSpawnerUnavailable]
    /// - [TaskErrorKind::TaskCancelled]
    /// - [TaskErrorKind::PreviousTaskPanic]
    /// - [TaskErrorKind::RuntimeShutdown]
    /// 
    /// [kind()]: Self::kind
    /// [TaskErrorKind::WorkerAborted]: crate::TaskErrorKind::WorkerAborted
    /// [TaskErrorKind::WorkerCancelled]: crate::TaskErrorKind::WorkerCancelled
    /// [TaskErrorKind::TaskSpawnerUnavailable]: crate::TaskErrorKind::TaskSpawnerUnavailable
    /// [TaskErrorKind::TaskCancelled]: crate::TaskErrorKind::TaskCancelled
    /// [TaskErrorKind::PreviousTaskPanic]: crate::TaskErrorKind::PreviousTaskPanic
    /// [TaskErrorKind::RuntimeShutdown]: crate::TaskErrorKind::RuntimeShutdown
    pub fn is_cancelled(&self) -> bool {
        let kind = self.kind();
        kind.is_worker_aborted() ||
        kind.is_worker_cancelled() ||
        kind.is_task_spawner_unavailable() ||
        kind.is_task_cancelled() ||
        kind.is_previous_task_panic() ||
        kind.is_runtime_shutdown()
    }

    /// Returns true if the error was caused by the task panicking.
    ///
    /// This occurs when [kind()] returns [TaskErrorKind::TaskPanic].
    /// 
    /// [kind()]: Self::kind
    /// [TaskErrorKind::TaskPanic]: crate::TaskErrorKind::TaskPanic
    pub fn is_panic(&self) -> bool {
        self.kind().is_task_panic()
    }

    /// Returns the [TaskErrorKind] corresponding to this error.
    /// 
    /// [TaskErrorKind]: crate::TaskErrorKind
    pub fn kind(&self) -> TaskErrorKind {
        match self.repr {
            Repr::WorkerAborted => TaskErrorKind::WorkerAborted,
            Repr::WorkerCancelled => TaskErrorKind::WorkerCancelled,
            Repr::TaskSpawnerUnavailable => TaskErrorKind::TaskSpawnerUnavailable,
            Repr::TaskCancelled => TaskErrorKind::TaskCancelled,
            Repr::TaskPanic { .. } => TaskErrorKind::TaskPanic,
            Repr::PrevTaskPanic { .. } => TaskErrorKind::PreviousTaskPanic,
            Repr::RuntimeShutdown => TaskErrorKind::RuntimeShutdown,
        }
    }
}

impl fmt::Debug for TaskError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::WorkerAborted => f.write_str("TaskError::WorkerAborted"),
            Repr::WorkerCancelled => f.write_str("TaskError::WorkerCancelled"),
            Repr::TaskSpawnerUnavailable => f.write_str("TaskError::TaskSpawnerUnavailable"),
            Repr::TaskCancelled => f.write_str("TaskError::TaskCancelled"),
            Repr::PrevTaskPanic { panic_msg } => {
                f.debug_struct("TaskError::PreviousTaskPanic")
                    .field("panic_msg", panic_msg)
                    .finish()
            },
            Repr::TaskPanic { panic } => {
                f.debug_struct("TaskError::TaskPanic")
                    .field("panic_msg", &panic.msg())
                    .finish()
            },
            Repr::RuntimeShutdown => f.write_str("TaskError::RuntimeShutdown"),
        }
    }
}

impl fmt::Display for TaskError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::WorkerAborted => f.write_str("worker was aborted"),
            Repr::WorkerCancelled => f.write_str("worker was cancelled"),
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
            },
            Repr::RuntimeShutdown => f.write_str("the Tokio runtime was shut down"),
        }
    }
}

impl std::error::Error for TaskError {}

/// The kind of error that occurred while waiting for a task to complete.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum TaskErrorKind {

    /// The error was caused by the worker being aborted.
    ///
    /// See [is_worker_aborted()] for details.
    /// 
    /// [is_worker_aborted()]: Self::is_worker_aborted
    WorkerAborted,

    /// The error was caused by the worker being cancelled.
    ///
    /// See [is_worker_cancelled()] for details.
    /// 
    /// [is_worker_cancelled()]: Self::is_worker_cancelled
    WorkerCancelled,

    /// The error was caused by a previous task panicking,
    /// which may have left the worker state in an invalid state.
    ///
    /// See [is_previous_task_panic()] for details.
    /// 
    /// [is_previous_task_panic()]: Self::is_previous_task_panic
    PreviousTaskPanic,

    /// The error was caused by an attempt to spawn the task
    /// after the [TaskSpawner] became unavailable.
    /// 
    /// See [is_task_spawner_unavailable()] for details.
    /// 
    /// [TaskSpawner]: crate::TaskSpawner
    /// [is_task_spawner_unavailable()]: Self::is_task_spawner_unavailable
    TaskSpawnerUnavailable,

    /// The error was caused by the task being cancelled through its handle.
    ///
    /// See [is_task_cancelled()] for details.
    /// 
    /// [is_task_cancelled()]: Self::is_task_cancelled
    TaskCancelled,

    /// The error was caused by the task panicking.
    ///
    /// See [is_task_panic()] for details.
    /// 
    /// [is_task_panic()]: Self::is_task_panic
    TaskPanic,

    /// The error was caused by the Tokio task for the worker being aborted due to the Tokio runtime shutting down.
    /// 
    /// See [is_runtime_shutdown()] for details.
    /// 
    /// [is_runtime_shutdown()]: Self::is_runtime_shutdown
    RuntimeShutdown,
}

impl TaskErrorKind {

    /// Returns true if the error was caused by the worker being aborted.
    ///
    /// Note that blocking tasks are not asynchronous, so if one is already running,
    /// aborting it only detaches the task from the [WorkerHandle];
    /// it continues running normally.
    /// In this case, its [TaskHandle] does not return this error.
    ///
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use std::time::Duration;
    /// use tokio::time::sleep;
    /// 
    /// let worker = async_sequential::spawn_worker(());
    ///
    /// let task = worker.spawn(move |_| Box::pin(async move {
    ///     // Never completes
    ///     std::future::pending::<()>().await;
    /// }));
    /// 
    /// sleep(Duration::from_secs(1)).await;
    /// worker.abort();
    ///
    /// // The task was aborted
    /// // because the worker was aborted before the task completed.
    /// let err = task.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.kind().is_worker_aborted());
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
    /// let worker = async_sequential::spawn_worker(());
    /// 
    /// let task1 = worker.spawn_blocking(move |_| {
    ///     thread::sleep(Duration::from_secs(2));
    ///     "complete"
    /// });
    /// let task2 = worker.spawn_blocking(move |_| {
    ///     unreachable!();
    /// });
    /// 
    /// sleep(Duration::from_secs(1)).await;
    /// worker.abort();
    /// 
    /// // The task was not aborted because it had already started.
    /// // Aborting the worker only detaches a running blocking task.
    /// assert_eq!(task1.await.unwrap(), "complete");
    /// 
    /// // The task was aborted 
    /// // because the blocking task was not started.
    /// let err = task2.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.kind().is_worker_aborted());
    /// # assert!(!err.kind().is_task_spawner_unavailable());
    /// # assert!(!err.kind().is_worker_cancelled());
    /// # assert!(!err.kind().is_task_cancelled());
    /// # assert!(!err.is_panic());
    /// # });
    /// # }
    /// ```
    /// 
    /// [WorkerHandle]: crate::WorkerHandle
    /// [TaskHandle]: crate::TaskHandle
    pub fn is_worker_aborted(&self) -> bool {
        matches!(self, TaskErrorKind::WorkerAborted)
    }

    /// Returns true if the error was caused by the worker being cancelled.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let worker = async_sequential::spawn_worker(());
    ///
    /// worker.spawn(move |_| Box::pin(async move {
    ///     // Never completes
    ///     std::future::pending::<()>().await;
    /// }));
    ///
    /// let task = worker.spawn(move |_| Box::pin(async move {
    ///     unreachable!();
    /// }));
    /// 
    /// worker.cancel();
    ///
    /// // The task was cancelled
    /// // by the worker before it started.
    /// let err = task.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.kind().is_worker_cancelled());
    /// # });
    /// # }
    /// ```
    pub fn is_worker_cancelled(&self) -> bool {
        matches!(self, TaskErrorKind::WorkerCancelled)
    }

    /// Returns true if the error was caused by a previous task panicking,
    /// which may have left the worker state in an invalid state.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let worker = async_sequential::spawn_worker(());
    ///
    /// // Task panic
    /// let task = worker.spawn(move |_| Box::pin(async move {
    ///     panic!()
    /// }));
    /// let err = task.await.unwrap_err();
    /// assert!(err.is_panic());
    /// assert!(err.kind().is_task_panic());
    ///
    /// // Subsequent tasks fail
    /// // because the state invariants may have been violated
    /// // by the previous task's panic.
    /// let task = worker.spawn(move |_| Box::pin(async move {
    ///     unreachable!();
    /// }));
    /// let err = task.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.kind().is_previous_task_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_previous_task_panic(&self) -> bool {
        matches!(self, TaskErrorKind::PreviousTaskPanic)
    }

    /// Returns true if the error was caused by an attempt to spawn the task
    /// after the [TaskSpawner] became unavailable.
    /// 
    /// See [TaskSpawner::is_unavailable()] for details.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use tokio::time::sleep;
    /// use std::time::Duration;
    ///
    /// let mut worker = async_sequential::spawn_worker(());
    ///
    /// let _ = worker.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    /// }));
    ///
    /// let spawner = worker.spawner();
    ///
    /// let task = tokio::spawn(async move {
    ///     sleep(Duration::from_secs(1)).await;
    ///     spawner.spawn(move |_| Box::pin(async move {
    ///         unreachable!();
    ///     }))
    /// });
    ///
    /// worker.close_spawners();
    /// worker.join().await;
    ///
    /// // The task was cancelled
    /// // because the spawner was closed before the task was spawned.
    /// let err = task.await.unwrap().await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.kind().is_task_spawner_unavailable());
    /// # });
    /// # }
    /// ```
    /// 
    /// [TaskSpawner::is_unavailable()]: TaskSpawner::is_unavailable
    pub fn is_task_spawner_unavailable(&self) -> bool {
        matches!(self, TaskErrorKind::TaskSpawnerUnavailable)
    }

    /// Returns true if the error was caused by the task being cancelled through its handle.
    ///
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let worker = async_sequential::spawn_worker(());
    ///
    /// worker.spawn(move |_| Box::pin(async move {
    ///     // Never completes
    ///     std::future::pending::<()>().await;
    /// }));
    ///
    /// let task = worker.spawn(move |_| Box::pin(async move {
    ///     unreachable!();
    /// }));
    /// task.cancel();
    ///
    /// // The task was cancelled
    /// // by its handle before it started.
    /// let err = task.await.unwrap_err();
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
    /// let worker = async_sequential::spawn_worker(());
    ///
    /// // Task panic
    /// let task = worker.spawn(move |_| Box::pin(async move {
    ///     panic!()
    /// }));
    /// let err = task.await.unwrap_err();
    /// assert!(err.is_panic());
    /// assert!(err.kind().is_task_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_task_panic(&self) -> bool {
        matches!(self, TaskErrorKind::TaskPanic)
    }

    /// Returns true if the error was caused by the Tokio task for the worker being aborted due to the Tokio runtime shutting down.
    pub fn is_runtime_shutdown(&self) -> bool {
        matches!(self, TaskErrorKind::RuntimeShutdown)
    }
}

impl fmt::Display for TaskErrorKind {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskErrorKind::WorkerAborted => f.write_str("worker was aborted"),
            TaskErrorKind::WorkerCancelled => f.write_str("worker was cancelled"),
            TaskErrorKind::TaskSpawnerUnavailable => f.write_str("task spawner was unavailable"),
            TaskErrorKind::TaskCancelled => f.write_str("task was cancelled"),
            TaskErrorKind::PreviousTaskPanic => f.write_str("previous task panicked"),
            TaskErrorKind::TaskPanic => f.write_str("task panicked"),
            TaskErrorKind::RuntimeShutdown => f.write_str("the Tokio runtime was shut down"),
        }
    }
}