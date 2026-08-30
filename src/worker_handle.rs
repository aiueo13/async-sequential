use crate::*;
use std::{future::Future, pin::Pin};


/// A handle to the worker for running asynchronous
/// and blocking tasks sequentially on a shared mutable state,
/// allowing the final state to be obtained after all tasks complete.
///
/// Tasks are executed sequentially in the order they are queued,
/// regardless of whether they are asynchronous or blocking.
/// If a task panics, subsequent tasks also fail because the state invariants
/// may have been violated by the task's panic.
///
/// # Worker
/// The worker does not provide an async runtime or its own thread pool.
/// Internally, the worker runs as a single Tokio task.
/// Each blocking task is executed on Tokio's blocking thread pool.
///
/// The worker will terminate when any of the following conditions is met:
/// - The worker is aborted.
/// - The worker is cancelled.
/// - This WorkerHandle is dropped, there are no available [TaskSpawner]s, and all queued tasks have completed.
/// 
/// # Examples
/// ```
/// # fn main() {
/// # tokio_test::block_on(async {
/// use std::{thread, time::Duration};
/// use tokio::time::sleep;
/// 
/// // Spawn a worker with the given state
/// let worker = async_sequential::spawn_worker(Vec::new());
///
/// // Spawn a task onto the worker
/// worker.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
///     sleep(Duration::from_secs(1)).await;
///     state.push(1);
/// }));
///
/// // A spawner can be used to spawn tasks onto the worker
/// // from another thread or Tokio task.
/// let spawner = worker.spawner();
/// tokio::spawn(async move {
///     let task_handle1 = spawner.spawn_blocking(move |state| {
///         thread::sleep(Duration::from_secs(2));
///         state.push(2);
///         "hello"
///     });
///     let task_handle2 = spawner.spawn(move |state| Box::pin(async move {
///         sleep(Duration::from_secs(1)).await;
///         state.push(3);
///         "world"
///     }));
/// 
///     assert_eq!(task_handle1.await.unwrap(), "hello");
///     assert_eq!(task_handle2.await.unwrap(), "world");
/// 
///     // Drop the spawner to allow the worker to complete.
///     drop(spawner);
/// });
///
/// // Wait for all tasks to complete.
/// // NOTE: This does not complete as long as any spawner remains alive.
/// let result = worker.join().await.unwrap();
/// assert_eq!(result, vec![1, 2, 3]);
/// # });
/// # }
/// ``` 
pub struct WorkerHandle<S> {
    repr: internal::WorkerHandle<S>,
}

impl<S> WorkerHandle<S> {

    pub(crate) fn new(repr: internal::WorkerHandle<S>) -> Self {
        Self { repr }
    }
}

impl<S> WorkerHandle<S> {

    /// Waits for all tasks to complete and returns the final state.
    ///
    /// Note that this method does not complete as long as any [TaskSpawner]
    /// obtained from [spawner()] remains alive.
    /// To allow it to complete, 
    /// either drop all TaskSpawners or call [close_spawners()] beforehand.
    /// 
    /// # Errors
    /// Returns an error if a task panicked or the Tokio task for the worker was aborted due to the Tokio runtime shutting down.
    /// The state can be retrieved using [PoisonError::into_inner], but note
    /// that the state may violate its invariants
    /// because a task terminated without completing normally after it started.
    /// 
    /// [spawner()]: Self::spawner
    /// [close_spawners()]: Self::close_spawners
    /// [TaskSpawner]: crate::TaskSpawner
    /// [PoisonError::into_inner]: crate::PoisonError::into_inner
    pub async fn join(self) -> Result<S, PoisonError<S>> {
        self.repr.join().await.map_err(Into::into)
    }

    /// Cancels all queued tasks, prevents subsequent tasks from being queued,
    /// and aborts the currently running task.
    ///
    /// Note that blocking tasks are not asynchronous, so if one is already running,
    /// aborting it only detaches the task from the WorkerHandle; 
    /// it continues running normally.
    pub fn abort(self) {
        self.repr.abort();
    }

    /// Cancels all queued tasks, prevents subsequent tasks from being queued,
    /// and allows the currently running task to complete.
    /// 
    /// This method **does not abort a running task**.
    /// To abort the currently running task, use [abort()].
    /// 
    /// [abort()]: Self::abort
    pub fn cancel(self) {
        self.repr.cancel();
    }

    /// Cancels all queued tasks, prevents subsequent tasks from being queued,
    /// aborts the currently running task,
    /// and returns a [PoisonError] containing the final state.
    /// 
    /// The state can be retrieved using [PoisonError::into_inner], but note
    /// that the state may violate its invariants.
    /// 
    /// Note that blocking tasks are not asynchronous, so if a blocking task is
    /// already running, this method waits for it to complete.
    /// 
    /// [PoisonError::into_inner]: crate::PoisonError::into_inner
    pub async fn abort_and_join(self) -> PoisonError<S> {
        self.repr.abort_and_join().await.into()
    }

    /// Cancels all queued tasks, prevents subsequent tasks from being queued,
    /// waits for the currently running task to complete,
    /// and returns the final state.
    /// 
    /// This method **does not abort a running task** to preserve the state invariant.
    /// 
    /// # Errors
    /// Returns an error if a task panicked or the Tokio task for the worker was aborted due to the Tokio runtime shutting down.
    /// The state can be retrieved using [PoisonError::into_inner], but note
    /// that the state may violate its invariants
    /// because a task terminated without completing normally after it started.
    /// 
    /// [PoisonError::into_inner]: crate::PoisonError::into_inner
    pub async fn cancel_and_join(self) -> Result<S, PoisonError<S>> {
        self.repr.cancel_and_join().await.map_err(Into::into)
    }

    /// Returns true if a task executed by the worker has panicked.
    /// 
    /// Once a task has panicked,
    /// [spawn()] or [spawn_blocking()] returns a [TaskHandle]
    /// that immediately resolves to an error
    /// for which [TaskError::kind()] is [TaskErrorKind::PreviousTaskPanic].
    /// This is because the state invariants may have been violated by the task's panic.
    /// 
    /// [spawn()]: Self::spawn
    /// [spawn_blocking()]: Self::spawn_blocking
    /// [TaskHandle]: crate::TaskHandle
    /// [TaskError::kind()]: crate::TaskError::kind
    /// [TaskErrorKind::PreviousTaskPanic]: crate::TaskErrorKind::PreviousTaskPanic
    pub fn has_panicked(&self) -> bool {
        self.repr.has_panicked()
    }

    /// Closes all [TaskSpawner]s currently associated with the worker.
    ///
    /// After this method is called, existing TaskSpawners can no longer spawn new tasks.
    /// Tasks that have already been queued or are currently running are unaffected.
    /// TaskSpawners obtained after this call are also unaffected.
    /// 
    /// [TaskSpawner]: crate::TaskSpawner
    pub fn close_spawners(&mut self) {
        self.repr.close_senders();
    }

    /// Returns a [TaskSpawner] for queuing tasks onto the worker.
    /// 
    /// Note that [join()] does not complete
    /// as long as any TaskSpawner remains alive.
    /// To allow it to complete, 
    /// either drop all TaskSpawners or call [close_spawners()] beforehand.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let worker = async_sequential::spawn_worker(());
    /// let spawner = worker.spawner();
    /// 
    /// spawner.spawn(move |_| Box::pin(async move {}));
    /// 
    /// // Ensures that the spawner is closed before join.
    /// drop(spawner);
    /// assert!(worker.join().await.is_ok());
    /// # });
    /// # }
    /// ```
    /// 
    /// [join()]: Self::join
    /// [close_spawners()]: Self::close_spawners
    pub fn spawner(&self) -> TaskSpawner<S> {
        TaskSpawner::new(self.repr.sender())
    }
}

impl<S: Send + 'static> WorkerHandle<S> {

    /// Queues an asynchronous task for sequential execution, 
    /// returning a [TaskHandle] to wait for it to complete.
    /// 
    /// This method does not panic or return an error
    /// even when the task should no longer be spawned or cannot be spawned. 
    /// Instead, the returned TaskHandle immediately completes with an error
    /// corresponding to the relevant [TaskErrorKind].
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let worker = async_sequential::spawn_worker(Vec::new());
    /// 
    /// worker.spawn(move |state| Box::pin(async move {
    ///     state.push(0);
    /// }));
    /// # });
    /// # }
    /// ```
    /// 
    /// [TaskErrorKind]: crate::TaskErrorKind
    /// [TaskHandle]: crate::TaskHandle
    pub fn spawn<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
        R: Send + 'static,
    {
        let (task, task_result, task_canceller) = internal::build_async_task(task);
        match self.repr.send(task) {
            Ok(worker_status) => TaskHandle::new(task_result, task_canceller, worker_status),
            Err(e) => TaskHandle::unspawned(e),
        }
    }

    /// Queues a blocking task for sequential execution, 
    /// returning a [TaskHandle] to wait for it to complete.
    ///
    /// The blocking task is executed using Tokio's blocking thread pool
    /// to avoid blocking the asynchronous runtime.
    /// 
    /// This method does not panic or return an error
    /// even when the task should no longer be spawned or cannot be spawned. 
    /// Instead, the returned TaskHandle immediately completes with an error
    /// corresponding to the relevant [TaskErrorKind].
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let worker = async_sequential::spawn_worker(Vec::new());
    /// 
    /// worker.spawn_blocking(move |state| {
    ///     state.push(0);
    /// });
    /// # });
    /// # }
    /// ```
    /// 
    /// [TaskErrorKind]: crate::TaskErrorKind
    /// [TaskHandle]: crate::TaskHandle
    pub fn spawn_blocking<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: (FnOnce(&mut S) -> R) + Send + 'static,
        R: Send + 'static,
    {
        let (task, task_result, task_canceller) = internal::build_blocking_task(task);
        match self.repr.send(task) {
            Ok(worker_status) => TaskHandle::new(task_result, task_canceller, worker_status),
            Err(e) => TaskHandle::unspawned(e),
        }
    }
}


#[cfg(test)]
mod asserts {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    fn require_send_static_unpin_unwindsafe<F: Send + 'static + Unpin + UnwindSafe + RefUnwindSafe>(_: F) {}
    fn require_send_static<F: Send + 'static>(_: F) {}

    #[allow(unused)]
    fn assert_impls() {
        let worker = crate::spawn_worker(());
        require_send_static_unpin_unwindsafe(worker);
        
        let worker = crate::spawn_worker(());
        require_send_static(worker.join());

        let worker = crate::spawn_worker(());
        require_send_static(worker.cancel_and_join());

        let worker = crate::spawn_worker(());
        require_send_static(worker.abort_and_join());

        let worker = crate::spawn_worker(());
        require_send_static(worker.spawn(|_| Box::pin(async {})));

        let worker = crate::spawn_worker(());
        require_send_static(worker.spawn_blocking(|_| {}));
    }
}