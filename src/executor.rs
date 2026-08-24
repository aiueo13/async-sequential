use crate::*;
use std::{future::Future, pin::Pin, sync::{Arc, Mutex as SyncMutex}};


/// Executor for running asynchronous and blocking tasks sequentially on a shared mutable state.
/// 
/// Tasks are executed sequentially in the order they are queued,
/// regardless of whether they are asynchronous or blocking.
/// 
/// If a task panics, subsequent tasks also panic because the state invariants
/// may have been violated by the task's panic.
/// 
/// When the Executor is dropped, all tasks in the Executor are immediately aborted.
/// Note that blocking tasks are not asynchronous, so if one is already running,
/// aborting it only detaches the task from the Executor; 
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

    /// Creates a new Executor with the given initial state.
    ///
    /// The state is owned by the Executor
    /// and is made available to tasks through exclusive mutable access.
    pub const fn new(state: S) -> Self {
        Self {
            worker: SyncMutex::new(Some(Worker::Unstarted { state }))
        }
    }

    /// Waits for all tasks to complete and returns the final state.
    ///
    /// Note that this method does not complete as long as any [TaskSpawner]
    /// obtained from [spawner](Executor::spawner) remains alive.
    /// To allow it to complete, either drop all TaskSpawners
    /// or call [close_spawners](Executor::close_spawners) beforehand.
    /// 
    /// # Panics
    /// Panics if any task panicked before or during this method,
    /// or if this method is called outside Tokio runtime.
    pub async fn join(self) -> S {
        self.try_join().await.unwrap_or_else(|e| e.panic())
    }

    /// Waits for all tasks to complete and returns the final state.
    ///
    /// Note that this method does not complete as long as any [TaskSpawner]
    /// obtained from [spawner](Executor::spawner) remains alive.
    /// To allow it to complete, either drop all TaskSpawners
    /// or call [close_spawners](Executor::close_spawners) beforehand.
    /// 
    /// # Errors
    /// Returns an error if any task panicked before or during this method.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub async fn try_join(self) -> Result<S, ExecutorJoinError> {
        let worker = self.worker.lock().unwrap().take();
        match worker {
            Some(Worker::Unstarted { state, .. }) => Ok(state),
            Some(Worker::Started { worker_handle }) => Ok(worker_handle.join().await?),
            None => unreachable!("illegal closed executor"),
        }
    }

    /// Cancels all queued tasks and prevents subsequent tasks from being queued, 
    /// detaches the currently running task from the Executor so that it can continue running.
    /// 
    /// This method **does not abort a running task**.
    /// To abort the currently running task, drop the Executor itself.
    /// Note that blocking tasks are not asynchronous, so if one is already running,
    /// aborting it only detaches the task from the Executor; 
    /// it continues running normally.
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

    /// Cancels all queued tasks and prevents subsequent tasks from being queued,
    /// waits for the currently running task to complete,
    /// and returns the final state.
    /// 
    /// This method **does not abort a running task** to preserve the state invariant.
    /// 
    /// # Panics
    /// Panics if any task panicked before or during this method,
    /// or if this method is called outside Tokio runtime.
    pub async fn cancel_and_join(self) -> S {
        self.try_cancel_and_join().await.unwrap_or_else(|e| e.panic())
    }

    /// Cancels all queued tasks and prevents subsequent tasks from being queued,
    /// waits for the currently running task to complete,
    /// and returns the final state.
    /// 
    /// This method **does not abort a running task** to preserve the state invariant.
    /// 
    /// # Errors
    /// Returns an error if any task panicked before or during this method.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub async fn try_cancel_and_join(self) -> Result<S, ExecutorJoinError> {
        let worker = self.worker.lock().unwrap().take();
        match worker {
            Some(Worker::Unstarted { state, .. }) => Ok(state),
            Some(Worker::Started { worker_handle }) => Ok(worker_handle.cancel_and_join().await?),
            None => unreachable!("illegal closed executor"),
        }
    }

    /// Closes all [TaskSpawner]s currently associated with this Executor.
    ///
    /// After this method is called, all existing TaskSpawners can no longer spawn new tasks.
    /// Tasks that have already been queued or are currently running are unaffected.
    /// TaskSpawners obtained after this call are also unaffected.
    pub fn close_spawners(&self) {
        let mut worker = self.worker.lock().unwrap();
        match &mut *worker {
            Some(Worker::Unstarted { .. }) => {},
            Some(Worker::Started { worker_handle }) => worker_handle.close_task_senders(),
            None => unreachable!("illegal closed executor"),
        }
    }
}

impl<S: Send + 'static> Executor<S> {

    /// Returns a [TaskSpawner] for queuing tasks onto this Executor.
    ///
    /// TaskSpawner provides methods equivalent to [spawn](Executor::spawn) and [spawn_blocking](Executor::spawn_blocking),
    /// except that the returned [TaskHandle] immediately returns an error
    /// if the TaskSpawner can no longer spawn tasks,
    /// such as when the TaskSpawner has been closed
    /// or the Executor has been aborted or cancelled.
    /// In such cases, [TaskError::is_task_spawner_unavailable] returns true.
    /// 
    /// Note that [join](Executor::join) and [try_join](Executor::try_join) does not complete as long as any TaskSpawner remains alive.
    /// To allow it to complete, either drop all TaskSpawners
    /// or call [close_spawners](Executor::close_spawners) beforehand.
    /// 
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(());
    /// let spawner = executor.spawner();
    /// 
    /// spawner.spawn(move |_| Box::pin(async move {}));
    /// 
    /// // Ensures that the spawner is closed before join.
    /// drop(spawner);
    /// assert!(executor.try_join().await.is_ok());
    /// # });
    /// # }
    /// ```
    pub fn spawner(&self) -> TaskSpawner<S> {
        TaskSpawner::new(self.sender())
    }

    /// Queues an asynchronous task for sequential execution, 
    /// returning a [TaskHandle] to wait for it to complete.
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
        match self.send(task) {
            Ok(worker_state) => TaskHandle::new(task_result, task_controller, worker_state),
            Err(WorkerSendError::PrevTaskPanic { panic_msg }) => TaskHandle::prev_task_already_panicked(panic_msg),
        }
    }

    /// Queues a blocking task for sequential execution, 
    /// returning a [TaskHandle] to wait for it to complete.
    ///
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    /// 
    /// The blocking task is executed using the runtime's blocking thread pool
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
        match self.send(task) {
            Ok(worker_state) => TaskHandle::new(task_result, task_controller, worker_state),
            Err(WorkerSendError::PrevTaskPanic { panic_msg }) => TaskHandle::prev_task_already_panicked(panic_msg),
        }
    }

    /// Queues an asynchronous task for sequential execution and waits for it to complete.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    /// 
    /// This is a convenient wrapper around [spawn](Executor::spawn).
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
    /// The blocking task is executed using the runtime's blocking thread pool
    /// to avoid blocking the asynchronous runtime.
    /// 
    /// This is a convenient wrapper around [spawn_blocking](Executor::spawn_blocking).
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


    fn send(&self, task: Task<S>) -> Result<Arc<WorkerState>, WorkerSendError> {
        let mut locked_worker = self.worker.lock().unwrap();

        if let Some(Worker::Started { ref worker_handle }) = *locked_worker {
            return worker_handle.send(task);
        }

        let Some(Worker::Unstarted { state }) = locked_worker.take() else {
            unreachable!("illegal closed executor")
        };

        let worker_handle = spawn_worker(state);
        let worker_state = match worker_handle.send(task) {
            Ok(worker_state) => worker_state,
            Err(WorkerSendError::PrevTaskPanic { .. }) => unreachable!(),
        };
        *locked_worker = Some(Worker::Started { worker_handle });
        Ok(worker_state)
    }

    fn sender(&self) -> WorkerTaskSender<S> {
        let mut locked_worker = self.worker.lock().unwrap();

        if let Some(Worker::Started { ref worker_handle }) = *locked_worker {
            return worker_handle.sender()
        }

        let Some(Worker::Unstarted { state }) = locked_worker.take() else {
            unreachable!("illegal closed executor")
        };

        let worker_handle = spawn_worker(state);
        let task_sender = worker_handle.sender();
        *locked_worker = Some(Worker::Started { worker_handle });
        task_sender
    }
}

impl<S: Default> Default for Executor<S> {

    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S> Drop for Executor<S> {

    fn drop(&mut self) {
        let worker = self.worker.lock().ok().and_then(|mut w| w.take());
        match worker {
            Some(Worker::Unstarted { .. }) => {},
            Some(Worker::Started { worker_handle }) => worker_handle.abort(),
            None => {},
        }
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