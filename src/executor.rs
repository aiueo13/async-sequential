use crate::*;
use crate::internal::*;
use std::pin::Pin;
use std::future::Future;
use std::sync::{Arc, Mutex as SyncMutex};


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
/// it continues running while holding the state.
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
    worker: SyncMutex<Option<Worker<S>>>,
}

enum Worker<S> {
    Unstarted {
        state: S,
    },
    Started {
        worker_handle: WorkerHandle<S>,
    },
}

impl<S> Executor<S> {

    /// Creates a new executor with the given initial state.
    ///
    /// The state is owned by the executor
    /// and is made available to tasks through exclusive mutable access.
    pub const fn new(state: S) -> Self {
        Self {
            worker: SyncMutex::new(Some(Worker::Unstarted { state })),
        }
    }

    /// Cancels all tasks that are neither finished **nor running**,
    /// detaching the currently running task from the [`Executor`]
    /// so that it can continue running.
    ///
    /// This method removes queued tasks from the executor
    /// and does not abort the currently running task.
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
        let worker = self.worker.lock().unwrap().take();
        match worker {
            Some(Worker::Unstarted { .. }) => {},
            Some(Worker::Started { worker_handle }) => worker_handle.cancel(),
            None => unreachable!("illegal closed executor"),
        }
    }

    /// Cancels all tasks that are neither finished **nor running**,
    /// waits for the currently running task to complete,
    /// and returns the final state.
    /// 
    /// This method removes queued tasks from the [`Executor`]
    /// and does not abort the currently running task to preserve the executor state invariant.
    /// 
    /// # Panics
    /// Panics if any task panicked before or during this method,
    /// or if this method is called outside Tokio runtime.
    pub async fn cancel_and_join(self) -> S {
        self.try_cancel_and_join().await.unwrap_or_else(|e| panic!("{e}"))
    }

    /// Cancels all tasks that are neither finished **nor running**,
    /// waits for the currently running task to complete,
    /// and returns the final state.
    /// 
    /// This method removes queued tasks from the [`Executor`]
    /// and does not abort the currently running task to preserve the executor state invariant.
    /// 
    /// # Errors
    /// Returns an error if any task panicked before or during this method.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub async fn try_cancel_and_join(self) -> Result<S, TaskError> {
        let worker = self.worker.lock().unwrap().take();
        match worker {
            Some(Worker::Unstarted { state }) => Ok(state),
            Some(Worker::Started { worker_handle }) => worker_handle.cancel_and_join().await,
            None => unreachable!("illegal closed executor"),
        }
    }

    /// Waits for all queued tasks to complete and returns the final state.
    ///
    /// # Panics
    /// Panics if any task panicked before or during this method,
    /// or if this method is called outside Tokio runtime.
    pub async fn join(self) -> S {
        self.try_join().await.unwrap_or_else(|e| panic!("{e}"))
    }

    /// Waits for all queued tasks to complete and returns the final state.
    ///
    /// # Errors
    /// Returns an error if any task panicked before or during this method.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub async fn try_join(self) -> Result<S, TaskError> {
        let worker = self.worker.lock().unwrap().take();
        match worker {
            Some(Worker::Unstarted { state }) => Ok(state),
            Some(Worker::Started { worker_handle }) => worker_handle.join().await,
            None => unreachable!("illegal closed executor"),
        }
    }
}

impl<S: Send + 'static> Executor<S> {

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
        self.spawn(task).await.unwrap_or_else(|e| panic!("{e}"))
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
        self.spawn_blocking(task).await.unwrap_or_else(|e| panic!("{e}"))
    }

    /// Queues an asynchronous task for sequential execution, 
    /// returning a [`TaskHandle`] to wait for it to complete.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    /// 
    /// When the [`Executor`] is dropped, all tasks in the executor are immediately aborted.
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
        match self.submit(task) {
            Ok(worker_flags) => TaskHandle::new(task_result, task_controller, worker_flags),
            Err(_) => TaskHandle::prev_task_panicked(),
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
    /// When the [`Executor`] is dropped, all tasks in the executor are immediately aborted.
    /// Note that blocking tasks are not asynchronous, so if one is already running,
    /// aborting it only detaches the task from the executor; 
    /// it continues running while holding the state.
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
        match self.submit(task) {
            Ok(worker_flags) => TaskHandle::new(task_result, task_controller, worker_flags),
            Err(_) => TaskHandle::prev_task_panicked(),
        }
    }


    fn submit(&self, task: Task<S>) -> Result<Arc<WorkerFlags>, ()> {
        let mut locked_worker = self.worker.lock().unwrap();

        if let Some(Worker::Started { ref worker_handle, .. }) = *locked_worker {
            return match worker_handle.send(task) {
                Ok(_) => Ok(worker_handle.flgas()),
                Err(_) => Err(())
            }
        }

        let Some(Worker::Unstarted { state }) = locked_worker.take() else {
            unreachable!("illegal closed executor")
        };

        let worker_handle = spawn_worker(state);
        worker_handle.send(task).unwrap();
        let worker_flags = worker_handle.flgas();

        *locked_worker = Some(Worker::Started { worker_handle });
        Ok(worker_flags)
    }
}

impl<S: Default> Default for Executor<S> {

    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S> Drop for Executor<S> {

    fn drop(&mut self) {
        match self.worker.lock().ok().and_then(|mut e| e.take()) {
            Some(Worker::Unstarted { .. }) => {},
            Some(Worker::Started { worker_handle, .. }) => worker_handle.abort(),
            None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    fn require_send_and_static<F: Future + Send + 'static>(_: F) {}
    fn require_send<F: Future + Send>(_: F) {}

    #[allow(unused)]
    fn assert_fn_future() {
        let executor = Executor::new(());
        require_send_and_static(executor.join());

        let executor = Executor::new(());
        require_send_and_static(executor.try_join());

        let executor = Executor::new(());
        require_send_and_static(executor.cancel_and_join());

        let executor = Executor::new(());
        require_send_and_static(executor.try_cancel_and_join());

        let executor = Executor::new(());
        require_send_and_static(executor.spawn(|_| Box::pin(async {})));

        let executor = Executor::new(());
        require_send_and_static(executor.spawn_blocking(|_| {}));

        let executor = Executor::new(());
        require_send(executor.execute(|_| Box::pin(async {})));

        let executor = Executor::new(());
        require_send(executor.execute_blocking(|_| {}));
    }
}