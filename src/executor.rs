use crate::*;
use crate::internal::*;
use std::pin::Pin;
use std::future::Future;


/// Executor for running asynchronous and blocking tasks sequentially on a shared mutable state.
/// 
/// Tasks are executed sequentially in the order they are queued,
/// regardless of whether they are asynchronous or blocking.
/// 
/// If a task panics, subsequent tasks also panic because the state invariants
/// may have been violated by the task's panic.
/// 
/// When the executor is dropped, all tasks in the executor are immediately aborted.
/// Note that blocking tasks are not asynchronous, so if one is already running,
/// aborting it only detaches the task from the executor; 
/// it continues running normally.
/// 
/// # Examples
/// ```
/// # fn main() {
/// # tokio_test::block_on(async {
/// let executor = async_sequential::Executor::new(Vec::new());
/// 
/// executor.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
///     state.push(identity(0).await);
/// }));
/// 
/// executor.spawn_blocking(move |state: &mut Vec<u64>| {
///     state.push(1);
/// });
/// 
/// let task_result = executor.execute(move |state: &mut Vec<u64>| Box::pin(async move {
///     state.push(identity(2).await);
///     "hello"
/// })).await;
/// assert_eq!(task_result, "hello");
/// 
/// let task_result = executor.execute_blocking(move |state: &mut Vec<u64>| {
///     state.push(3);
///     "world"
/// }).await;
/// assert_eq!(task_result, "world");
/// 
/// let result = executor.join().await;
/// assert_eq!(result, vec![0, 1, 2, 3]);
/// # });
/// # }
/// 
/// 
/// async fn identity(v: u64) -> u64 {
///     v
/// }
/// ```
pub struct Executor<S> {
    worker: Worker<S>,
}

impl<S> Executor<S> {

    /// Creates a new executor with the given initial state.
    ///
    /// The state is owned by the executor
    /// and is made available to tasks through exclusive mutable access.
    pub const fn new(state: S) -> Self {
        Self {
            worker: Worker::new(state),
        }
    }

    /// Cancels all queued tasks and detaches the currently running task
    /// from the executor so that it can continue running.
    /// 
    /// This method removes tasks from the queue if they have not started running
    /// and **does not abort a running task**.
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
    /// let running = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    ///     "complete"
    /// }));
    /// let pending = executor.spawn(move |_| Box::pin(async move {
    ///     "never"
    /// }));
    /// 
    /// sleep(Duration::from_secs(1)).await;
    /// executor.cancel();
    /// assert!(pending.await.unwrap_err().is_cancelled());
    /// assert_eq!(running.await.unwrap(), "complete");
    /// # });
    /// # }
    /// ```
    pub fn cancel(self) {
        self.worker.cancel();
    }

    /// Cancels all queued tasks,
    /// waits for the currently running task to complete,
    /// and returns the final state.
    /// 
    /// This method removes tasks from the queue if they have not started running
    /// and **does not abort a running task** to preserve the executor state invariant.
    /// 
    /// # Panics
    /// Panics if any task panicked before or during this method,
    /// or if this method is called outside Tokio runtime.
    pub async fn cancel_and_join(self) -> S {
        self.try_cancel_and_join().await.unwrap_or_else(|e| e.panic())
    }

    /// Cancels all queued tasks,
    /// waits for the currently running task to complete,
    /// and returns the final state.
    /// 
    /// This method removes tasks from the queue if they have not started running
    /// and **does not abort a running task** to preserve the executor state invariant.
    /// 
    /// # Errors
    /// Returns an error if any task panicked before or during this method.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub async fn try_cancel_and_join(self) -> Result<S, ExecutorJoinError> {
        self.worker.cancel_and_join().await.map_err(Into::into)
    }

    /// Waits for all tasks to complete and returns the final state.
    ///
    /// # Panics
    /// Panics if any task panicked before or during this method,
    /// or if this method is called outside Tokio runtime.
    pub async fn join(self) -> S {
        self.try_join().await.unwrap_or_else(|e| e.panic())
    }

    /// Waits for all tasks to complete and returns the final state.
    ///
    /// # Errors
    /// Returns an error if any task panicked before or during this method.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub async fn try_join(self) -> Result<S, ExecutorJoinError> {
        self.worker.join().await.map_err(Into::into)
    }
}

impl<S: Send + 'static> Executor<S> {

    /// Returns a [`TaskSpawner`] for queuing tasks onto this executor.
    ///
    /// The returned [`TaskSpawner`] can be used to spawn tasks without retaining the [`Executor`] itself.
    ///
    /// It provides methods equivalent to [`Executor::spawn`] and [`Executor::spawn_blocking`],
    /// except that the returned [`TaskHandle`] immediately returns an error
    /// if the executor is no longer available,
    /// such as when the executor has started joining, been aborted, or been cancelled.
    pub fn spawner(&self) -> TaskSpawner<S> {
        TaskSpawner::new(self.worker.weak_sender())
    }

    /// Queues an asynchronous task for sequential execution and waits for it to complete.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    /// 
    /// # Panics
    /// Panics if the task or any previous task panicked,
    /// or if this method is called outside Tokio runtime.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(Vec::new());
    /// 
    /// executor.execute(move |state| Box::pin(async move {
    ///     state.push(0);
    /// })).await;
    /// # });
    /// # }
    /// ```
    pub async fn execute<T, R>(&self, task: T) -> R
    where
        T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
        R: Send + 'static,
    {
        self.spawn(task).await.unwrap_or_else(|e| e.panic())
    }

    /// Queues a blocking task for sequential execution and waits for it to complete.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    /// 
    /// The blocking task is executed using blocking thread pool
    /// to avoid blocking the asynchronous runtime.
    /// 
    /// # Panics
    /// Panics if the task or any previous task panicked,
    /// or if this method is called outside Tokio runtime.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(Vec::new());
    /// 
    /// executor.execute_blocking(move |state| {
    ///     state.push(0);
    /// }).await;
    /// # });
    /// # }
    /// ```
    pub async fn execute_blocking<T, R>(&self, task: T) -> R
    where
        T: (FnOnce(&mut S) -> R) + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_blocking(task).await.unwrap_or_else(|e| e.panic())
    }

    /// Queues an asynchronous task for sequential execution, 
    /// returning a [`TaskHandle`] to wait for it to complete.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(Vec::new());
    /// 
    /// executor.spawn(move |state| Box::pin(async move {
    ///     state.push(0);
    /// }));
    /// # });
    /// # }
    /// ```
    pub fn spawn<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
        R: Send + 'static,
    {
        let (task, task_result, task_controller) = build_async_task(task);
        match self.worker.send(task) {
            Ok(worker_state) => TaskHandle::new(task_result, task_controller, worker_state),
            Err(WorkerSendError::PrevTaskPanic { panic_msg }) => TaskHandle::prev_task_panicked(panic_msg),
        }
    }

    /// Queues a blocking task for sequential execution, 
    /// returning a [`TaskHandle`] to wait for it to complete.
    ///
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    /// 
    /// The blocking task is executed using blocking thread pool
    /// to avoid blocking the asynchronous runtime.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(Vec::new());
    /// 
    /// executor.spawn_blocking(move |state| {
    ///     state.push(0);
    /// });
    /// # });
    /// # }
    /// ```
    pub fn spawn_blocking<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: (FnOnce(&mut S) -> R) + Send + 'static,
        R: Send + 'static,
    {
        let (task, task_result, task_controller) = build_blocking_task(task);
        match self.worker.send(task) {
            Ok(worker_state) => TaskHandle::new(task_result, task_controller, worker_state),
            Err(WorkerSendError::PrevTaskPanic { panic_msg }) => TaskHandle::prev_task_panicked(panic_msg),
        }
    }
}

impl<S: Default> Default for Executor<S> {

    fn default() -> Self {
        Self::new(S::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn require_send_static<F: Send + 'static>(_: F) {}
    fn require_send<F: Send>(_: F) {}

    #[allow(unused)]
    fn assert_impls() {
        let executor = Executor::new(());
        require_send_static(executor.join());

        let executor = Executor::new(());
        require_send_static(executor.try_join());

        let executor = Executor::new(());
        require_send_static(executor.cancel_and_join());

        let executor = Executor::new(());
        require_send_static(executor.try_cancel_and_join());

        let executor = Executor::new(());
        require_send_static(executor.spawn(|_| Box::pin(async {})));

        let executor = Executor::new(());
        require_send_static(executor.spawn_blocking(|_| {}));

        let executor = Executor::new(());
        require_send(executor.execute(|_| Box::pin(async {})));

        let executor = Executor::new(());
        require_send(executor.execute_blocking(|_| {}));
        require_send(executor);
    }
}