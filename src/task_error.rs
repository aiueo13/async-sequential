use std::fmt;


/// Error that occurred while waiting for a task to complete.
pub struct TaskError {
    repr: TaskErrorRepr,
}

enum TaskErrorRepr {
    Cancelled,
    Panic,
}

impl TaskError {

    pub(crate) fn panicked() -> Self {
        Self { repr: TaskErrorRepr::Panic }
    }

    pub(crate) fn cancelled() -> Self {
        Self { repr: TaskErrorRepr::Cancelled }
    }
}

impl TaskError {

    /// Returns `true` if the error was caused by the task being canceled.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use std::time::Duration;
    /// use tokio::time::sleep;
    /// 
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// // The task is canceled
    /// // when the handle cancels it before running.
    /// let _ = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    /// let handle = executor.spawn(move |_| Box::pin(async move { }));
    /// handle.cancel();
    /// assert!(handle.await.unwrap_err().is_cancelled());
    /// 
    /// // The task is aborted
    /// // when the executor is dropped before the task completes.
    /// let handle = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    /// drop(executor);
    /// assert!(handle.await.unwrap_err().is_cancelled());
    /// # });
    /// # }
    /// ```
    pub fn is_cancelled(&self) -> bool {
        matches!(self.repr, TaskErrorRepr::Cancelled)
    }

    /// Returns `true` if the error was caused by the task or any previous task panicking.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// let handle = executor.spawn(move |_| Box::pin(async move {
    ///     panic!()
    /// }));
    /// assert!(handle.await.unwrap_err().is_panic());
    /// 
    /// // Subsequent tasks also panic
    /// // because the state invariants may have been violated
    /// // by the preceding task's panic.
    /// let handle = executor.spawn(move |_| Box::pin(async move { }));
    /// assert!(handle.await.unwrap_err().is_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_panic(&self) -> bool {
        matches!(self.repr, TaskErrorRepr::Panic)
    }
}

impl fmt::Debug for TaskError {

    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            TaskErrorRepr::Cancelled => write!(fmt, "TaskError::Cancelled"),
            TaskErrorRepr::Panic => write!(fmt, "TaskError::Panic"),
        }
    }
}

impl fmt::Display for TaskError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.repr {
            TaskErrorRepr::Cancelled => write!(f, "task was cancelled"),
            TaskErrorRepr::Panic => write!(f, "task panicked"),
        }
    }
}

impl std::error::Error for TaskError {}

impl From<TaskError> for std::io::Error {

    fn from(value: TaskError) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            match value.repr {
                TaskErrorRepr::Cancelled => "task was cancelled",
                TaskErrorRepr::Panic => "task panicked",
            },
        )
    }
}