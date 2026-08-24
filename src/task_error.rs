use crate::*;
use std::{any::Any, fmt, sync::Arc};


/// Error that occurred while waiting for a task to complete.
pub struct TaskError {
    repr: Repr,
}

enum Repr {
    ExecutorAborted,
    ExecutorCancelled,
    TaskSpawnerUnavailable,
    TaskCancelled,
    TaskPanic {
        panic: PanicPayload
    },
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>
    },
}

impl TaskError {

    pub(crate) fn task_panicked(panic: PanicPayload) -> Self {
        Self { repr: Repr::TaskPanic { panic } }
    }

    pub(crate) fn prev_task_panicked(panic_msg: Option<Arc<String>>) -> Self {
        Self { repr: Repr::PrevTaskPanic { panic_msg } }
    }

    pub(crate) fn worker_aborted() -> Self {
        Self { repr: Repr::ExecutorAborted }
    }

    pub(crate) fn worker_cancelled() -> Self {
        Self { repr: Repr::ExecutorCancelled }
    }

    pub(crate) fn worker_task_sender_unavailable() -> Self {
        Self { repr: Repr::TaskSpawnerUnavailable }
    }

    pub(crate) fn task_cancelled() -> Self {
        Self { repr: Repr::TaskCancelled }
    }

    pub(crate) fn panic(self) -> ! {
        match self.repr {
            Repr::ExecutorAborted => panic!("executor was aborted"),
            Repr::ExecutorCancelled => panic!("executor was cancelled"),
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
            Repr::ExecutorAborted => panic!("cannot extract panic payload: executor was aborted"),
            Repr::ExecutorCancelled => panic!("cannot extract panic payload: executor was cancelled"),
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
            Repr::ExecutorAborted => Err(self),
            Repr::ExecutorCancelled => Err(self),
            Repr::TaskSpawnerUnavailable => Err(self),
            Repr::TaskCancelled => Err(self),
            Repr::TaskPanic { panic } => Ok(panic.into_inner()),
            Repr::PrevTaskPanic { .. } => Err(self),
        }
    }

    /// Returns true if the error was caused by the task was cancelled, either explicitly or implicitly.
    /// 
    /// It occurs when any of the following are true:
    /// - [is_executor_aborted](Self::is_executor_aborted)
    /// - [is_executor_cancelled](Self::is_executor_cancelled)
    /// - [is_task_spawner_unavailable](Self::is_task_spawner_unavailable)
    /// - [is_task_cancelled](Self::is_task_cancelled)
    pub fn is_cancelled(&self) -> bool {
        self.is_executor_aborted() ||
        self.is_executor_cancelled() ||
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
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// // Task panic
    /// let handle = executor.spawn(move |_| Box::pin(async move {
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
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// // Task panic
    /// let handle = executor.spawn(move |_| Box::pin(async move {
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
    /// let handle = executor.spawn(move |_| Box::pin(async move {
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

    /// Returns true if the error was caused by the [Executor](crate::Executor) being aborted.
    /// 
    /// It occurs when the task is aborted as a result of the executor being dropped.  
    /// 
    /// Note that blocking tasks are not asynchronous, so if one is already running,
    /// aborting it only detaches the task from the executor;
    /// it continues running normally.
    /// In this case, its [TaskHandle](crate::TaskHandle) does not return this error.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// let running = executor.spawn(move |_| Box::pin(async move {
    ///     // Never completes
    ///     std::future::pending::<()>().await;
    /// }));
    /// drop(executor);
    /// 
    /// // The task was aborted 
    /// // because the executor was dropped before the task completed.
    /// let err = running.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_executor_aborted());
    /// # assert!(!err.is_task_spawner_unavailable());
    /// # assert!(!err.is_executor_cancelled());
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
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// let running = executor.spawn_blocking(move |_| {
    ///     thread::sleep(Duration::from_secs(2));
    ///     "complete"
    /// });
    /// let queued = executor.spawn_blocking(move |_| {
    ///     unreachable!();
    /// });
    /// 
    /// sleep(Duration::from_secs(1)).await;
    /// drop(executor);
    /// 
    /// // The task was not aborted 
    /// // because the blocking task was started.
    /// assert_eq!(running.await.unwrap(), "complete");
    /// 
    /// // The task was aborted 
    /// // because the blocking task was not started.
    /// let err = queued.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_executor_aborted());
    /// # assert!(!err.is_task_spawner_unavailable());
    /// # assert!(!err.is_executor_cancelled());
    /// # assert!(!err.is_task_cancelled());
    /// # assert!(!err.is_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_executor_aborted(&self) -> bool {
        matches!(&self.repr, Repr::ExecutorAborted)
    }

    /// Returns true if the error was caused by the [Executor](crate::Executor) being cancelled.
    /// 
    /// It occurs when the task is cancelled as a result of [Executor::cancel](crate::Executor::cancel) being called.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// let _running = executor.spawn(move |_| Box::pin(async move {
    ///     // Never completes
    ///     std::future::pending::<()>().await;
    /// }));
    /// 
    /// let queued = executor.spawn(move |_| Box::pin(async move {
    ///     unreachable!();
    /// }));
    /// executor.cancel();
    /// # // Executor キャンセル後にタスクをキャンセルしても
    /// # // err.is_task_cancelled() は true にならないべき
    /// # queued.cancel();
    /// 
    /// // The task was cancelled 
    /// // by its handle before it started.
    /// let err = queued.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_executor_cancelled());
    /// # assert!(!err.is_executor_aborted());
    /// # assert!(!err.is_task_spawner_unavailable());
    /// # assert!(!err.is_task_cancelled());
    /// # assert!(!err.is_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_executor_cancelled(&self) -> bool {
        matches!(&self.repr, Repr::ExecutorCancelled)
    }

    /// Returns true if the error was caused by the task being spawned
    /// after the [TaskSpawner] could no longer spawn tasks.
    /// 
    /// It occurs when the task is cancelled before the task is spawned by the TaskSpawner as a result of any of the following:
    /// - [Executor] being aborted before spawning tasks.
    /// - Executor being cancelled before spawning tasks.
    /// - TaskSpawner being closed before spawning tasks.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use tokio::{task::spawn, time::sleep};
    /// use std::time::Duration;
    /// 
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// let _ = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    /// }));
    /// 
    /// let spawner = executor.spawner();
    /// 
    /// let task = spawn(async move {
    ///     sleep(Duration::from_secs(1)).await;
    ///     spawner.spawn(move |_| Box::pin(async move {
    ///         unreachable!();
    ///     }))
    /// });
    /// 
    /// executor.close_spawners();
    /// executor.join().await;
    /// 
    /// // The task was cancelled 
    /// // because the spawner was closed before the task was spawned.
    /// let err = task.await.unwrap().await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_task_spawner_unavailable());
    /// # assert!(!err.is_executor_aborted());
    /// # assert!(!err.is_executor_cancelled());
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
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// let _running = executor.spawn(move |_| Box::pin(async move {
    ///     // Never completes
    ///     std::future::pending::<()>().await;
    /// }));
    /// 
    /// let queued = executor.spawn(move |_| Box::pin(async move {
    ///     unreachable!();
    /// }));
    /// queued.cancel();
    /// # // タスクキャンセル後に Executor　をキャンセルしても
    /// # // err.is_executor_cancelled() は true にならないべき
    /// # executor.cancel();
    /// 
    /// // The task was cancelled 
    /// // by its handle before it started.
    /// let err = queued.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_task_cancelled());
    /// # assert!(!err.is_executor_aborted());
    /// # assert!(!err.is_task_spawner_unavailable());
    /// # assert!(!err.is_executor_cancelled());
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
            Repr::ExecutorAborted => f.write_str("TaskError::ExecutorAborted"),
            Repr::ExecutorCancelled => f.write_str("TaskError::ExecutorCancelled"),
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
            Repr::ExecutorAborted => f.write_str("executor was aborted"),
            Repr::ExecutorCancelled => f.write_str("executor was cancelled"),
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