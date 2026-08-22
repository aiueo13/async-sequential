use crate::*;
use std::{any::Any, fmt, sync::Arc};


/// Error that occurred while waiting for a task to complete.
pub struct TaskError {
    repr: Repr,
}

enum Repr {
    WorkerAborted,
    WorkerCancelled,
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
        Self { repr: Repr::WorkerAborted }
    }

    pub(crate) fn worker_cancelled() -> Self {
        Self { repr: Repr::WorkerCancelled }
    }

    pub(crate) fn task_cancelled() -> Self {
        Self { repr: Repr::TaskCancelled }
    }

    pub(crate) fn panic(self) -> ! {
        match self.repr {
            Repr::WorkerAborted => panic!("worker was aborted"),
            Repr::WorkerCancelled => panic!("worker was cancelled"),
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

    /// Returns `true` if the error was caused by the task being cancelled.
    /// 
    /// This returns `true` if any of the following are `true`:
    /// - [`is_worker_aborted`](Self::is_worker_aborted)
    /// - [`is_worker_cancelled`](Self::is_worker_cancelled)
    /// - [`is_task_cancelled`](Self::is_task_cancelled)
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use std::time::Duration;
    /// use tokio::time::sleep;
    /// 
    /// // The task is cancelled
    /// // when the handle cancels it before running.
    /// let executor = async_sequential::Executor::new(());
    /// let _ = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    /// let handle = executor.spawn(move |_| Box::pin(async move { }));
    /// handle.cancel();
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_task_cancelled());
    /// assert!(!err.is_worker_cancelled());
    /// assert!(!err.is_worker_aborted());
    /// 
    /// // The task is cancelled
    /// // when the executor is cancelled before the task completes.
    /// let executor = async_sequential::Executor::new(());
    /// let handle = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    /// executor.cancel();
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_worker_cancelled());
    /// assert!(!err.is_worker_aborted());
    /// assert!(!err.is_task_cancelled());
    /// 
    /// // The task is aborted
    /// // when the executor is dropped before the task completes.
    /// let executor = async_sequential::Executor::new(());
    /// let handle = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    /// drop(executor);
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(err.is_worker_aborted());
    /// assert!(!err.is_task_cancelled());
    /// assert!(!err.is_worker_cancelled());
    /// # });
    /// # }
    /// ```
    pub fn is_cancelled(&self) -> bool {
        self.is_worker_aborted() || self.is_worker_cancelled() || self.is_task_cancelled()
    }

    /// Returns `true` if the error was caused by the task or any previous task panicking.
    /// 
    /// This returns `true` if any of the following are `true`:
    /// - [`is_task_panic`](Self::is_task_panic)
    /// - [`is_prev_task_panic`](Self::is_prev_task_panic)
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
    /// let handle = executor.spawn(move |_| Box::pin(async move { }));
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_panic());
    /// assert!(err.is_prev_task_panic());
    /// assert!(!err.is_task_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_panic(&self) -> bool {
        self.is_task_panic() || self.is_prev_task_panic()
    }

    /// Extracts the original panic payload
    /// if the error was caused by the task itself panicking.
    ///
    /// The returned panic payload can be passed to
    /// [`std::panic::resume_unwind`] to resume the original panic.
    ///
    /// # Panics
    /// Panics if the error was not caused by the task itself panicking.
    /// In particular, the panic payload cannot be retrieved
    /// if the error was caused by a previous task panicking.
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        match self.repr {
            Repr::WorkerAborted => panic!("cannot extract panic payload: worker was aborted"),
            Repr::WorkerCancelled => panic!("cannot extract panic payload: worker was cancelled"),
            Repr::TaskCancelled => panic!("cannot extract panic payload: task was cancelled"),
            Repr::TaskPanic { panic } => panic.into_inner(),
            Repr::PrevTaskPanic { .. } => panic!("cannot extract panic payload: a previous task panicked"),
        }
    }

    /// Attempts to extract the original panic payload
    /// if the error was caused by the task itself panicking.
    ///
    /// The returned panic payload can be passed to
    /// [`std::panic::resume_unwind`] to resume the original panic.
    ///
    /// # Errors
    /// Returns the original error if it was not caused by the task itself panicking.
    /// In particular, the panic payload cannot be retrieved
    /// if the error was caused by a previous task panicking.
    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, Self> {
        match self.repr {
            Repr::WorkerAborted => Err(self),
            Repr::WorkerCancelled => Err(self),
            Repr::TaskCancelled => Err(self),
            Repr::TaskPanic { panic } => Ok(panic.into_inner()),
            Repr::PrevTaskPanic { .. } => Err(self),
        }
    }

    /// Returns `true` if the error was caused by the task panicking.
    pub fn is_task_panic(&self) -> bool {
        matches!(&self.repr, Repr::TaskPanic { .. })
    }

    /// Returns `true` if the error was caused by any previous task panicking.
    pub fn is_prev_task_panic(&self) -> bool {
        matches!(&self.repr, Repr::PrevTaskPanic { .. })
    }

    /// Returns `true` if the error was caused by the executor being dropped.
    pub fn is_worker_aborted(&self) -> bool {
        matches!(&self.repr, Repr::WorkerAborted)
    }

    /// Returns `true` if the error was caused by the worker being cancelled.
    pub fn is_worker_cancelled(&self) -> bool {
        matches!(&self.repr, Repr::WorkerCancelled)
    }

    /// Returns `true` if the error was caused by the task being cancelled in any of the following ways:
    /// 
    /// - [`TaskHandle::cancel`](crate::TaskHandle::cancel)
    /// - [`TaskCanceller::cancel`](crate::TaskCanceller::cancel)
    pub fn is_task_cancelled(&self) -> bool {
        matches!(&self.repr, Repr::TaskCancelled)
    }
}

impl fmt::Debug for TaskError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::WorkerAborted => f.write_str("TaskError::WorkerAborted"),
            Repr::WorkerCancelled => f.write_str("TaskError::WorkerCancelled"),
            Repr::TaskCancelled => f.write_str("TaskError::TaskCancelled"),
            Repr::PrevTaskPanic { panic_msg } => {
                f.debug_struct("TaskError::PreviousTaskPanic")
                    .field("panic_msg", panic_msg)
                    .finish()
            }
            Repr::TaskPanic { panic } => {
                f.debug_struct("TaskError::TaskPanic")
                    .field("panic_msg", &panic.as_str())
                    .finish()
            }
        }
    }
}

impl fmt::Display for TaskError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::WorkerAborted => f.write_str("worker was aborted"),
            Repr::WorkerCancelled => f.write_str("worker was cancelled"),
            Repr::TaskCancelled => f.write_str("task was cancelled"),
            Repr::PrevTaskPanic { panic_msg } => {
                match panic_msg {
                    Some(msg) => write!(f, "previous task panicked: {msg}"),
                    None => f.write_str("previous task panicked"),
                }
            }
            Repr::TaskPanic { panic } => {
                match panic.as_str() {
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
        let error = match &value.repr {
            Repr::WorkerCancelled => "worker was cancelled",
            Repr::WorkerAborted => "worker was aborted",
            Repr::TaskCancelled => "task was cancelled",
            Repr::PrevTaskPanic { .. } => "previous task panicked",
            Repr::TaskPanic { .. } => "task panicked",
        };

        std::io::Error::new(std::io::ErrorKind::Other, error)
    }
}