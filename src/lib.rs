use std::pin::Pin;
use std::future::Future;
use std::sync::{Arc, Mutex as SyncMutex, LazyLock, atomic::{AtomicBool, Ordering}};
use std::fmt;
use tokio::{task::{spawn, spawn_blocking, JoinHandle as SpawnJoinHandle}, sync::{oneshot, mpsc}};


/// Executor for running asynchronous and blocking tasks sequentially on a shared mutable state.
/// 
/// Tasks are executed sequentially in the order they are queued,
/// regardless of whether they are asynchronous or blocking.
/// 
/// If a task panics, subsequent tasks also panic because the state invariants
/// may have been violated by the task's panic.
/// 
/// When the executor is dropped, all tasks in the executor are immediately aborted.
/// Note blocking tasks are not asynchronous, so if one is already running,
/// aborting it only detaches it from a [`TaskHandle`],
/// and it continues running while holding the state.
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
    executor: SyncMutex<Option<ExecutorState<S>>>,
    is_aborted: LazyLock<Arc<AtomicBool>>
}

enum ExecutorState<S> {
    Idle {
        state: S,
    },
    Running {
        task_tx: mpsc::UnboundedSender<Task<S>>,
        handle: SpawnJoinHandle<S>,
    },
}

impl<S> Executor<S> {

    /// Creates a new executor with the given initial state.
    ///
    /// The state is owned by the executor
    /// and is made available to tasks through exclusive mutable access.
    pub const fn new(state: S) -> Self {
        Self {
            executor: SyncMutex::new(Some(ExecutorState::Idle { state })),
            is_aborted: LazyLock::new(|| Arc::new(AtomicBool::new(false))),
        }
    }
}

impl<S: Default> Default for Executor<S> {

    fn default() -> Self {
        Self::new(S::default())
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
    /// When a [`Executor`] is dropped, all tasks in the executor are immediately aborted.
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
        let is_executor_aborted = Arc::clone(&self.is_aborted);
        let (tx, rx) = oneshot::channel();
        let task = Task::Async(Box::new(|s: &mut S| Box::pin(async {
            let _ = tx.send(task(s).await);
        })));
        
        match self.submit(task) {
            Ok(_) => TaskHandle { state: TaskHandleState::Pending { rx, is_executor_aborted } },
            Err(_) => TaskHandle { state: TaskHandleState::PrevTaskPanic },
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
    /// When a [`Executor`] is dropped, all tasks in the executor are immediately aborted.
    /// Note blocking tasks are not asynchronous, so if one is already running,
    /// aborting it only detaches it from the [`TaskHandle`],
    /// and it continues running while holding the state.
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
        let is_executor_aborted = Arc::clone(&self.is_aborted);
        let (tx, rx) = oneshot::channel();
        let task = Task::Blocking(Box::new(move |s: &mut S| {
            let _ = tx.send(task(s));
        }));
        
        match self.submit(task) {
            Ok(_) => TaskHandle { state: TaskHandleState::Pending { rx, is_executor_aborted } },
            Err(_) => TaskHandle { state: TaskHandleState::PrevTaskPanic },
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
        match self.executor.lock().unwrap().take() {
            Some(ExecutorState::Idle { state }) => Ok(state),
            Some(ExecutorState::Running { handle, task_tx, .. }) => {
                drop(task_tx);
                match handle.await {
                    Ok(state) => Ok(state),
                    Err(e) if e.is_cancelled() => Err(TaskError { kind: TaskErrorKind::Cancelled }),
                    Err(_) => Err(TaskError { kind: TaskErrorKind::Panic })
                }
            }
            None => unreachable!("illegal closed executor"),
        }
    }


    fn submit(&self, task: Task<S>) -> Result<(), ()> {
        let mut locked_executor = self.executor.lock().unwrap();

        if let Some(ExecutorState::Running { ref task_tx, .. }) = *locked_executor {
            return match task_tx.send(task) {
                Ok(_) => Ok(()),
                Err(_) => Err(())
            }
        }

        *locked_executor = {
            let Some(ExecutorState::Idle { mut state }) = locked_executor.take() else {
                unreachable!("illegal closed executor")
            };

            let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task<S>>();
            task_tx.send(task).unwrap();

            let handle = spawn(async move {
                while let Some(task) = task_rx.recv().await {
                    match task {
                        Task::Blocking(task) => {
                            state = spawn_blocking(move || {
                                task(&mut state);
                                state
                            }).await.expect("blocking task unexpectedly failed");
                        },
                        Task::Async(task) => task(&mut state).await,
                    }
                }
                state
            });

            Some(ExecutorState::Running { task_tx, handle })
        };

        Ok(())
    }
}

impl<S> Drop for Executor<S> {

    fn drop(&mut self) {
        match self.executor.lock().ok().and_then(|mut e| e.take()) {
            Some(ExecutorState::Idle { .. }) => {},
            Some(ExecutorState::Running { handle, .. }) => {
                self.is_aborted.store(true, Ordering::Release);
                handle.abort();
            },
            None => {}
        }
    }
}

enum Task<S> {
    Blocking(Box<dyn (FnOnce(&mut S) -> ()) + Send>),
    Async(Box<dyn (for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>) + Send>),
}

/// Handle for waiting for a queued task to complete.  
///
/// Awaiting the handle returns the task's result if the task completes successfully,
/// or a [`TaskError`] if the task could not complete.
pub struct TaskHandle<R> {
    state: TaskHandleState<R>,
}

enum TaskHandleState<R> {
    PrevTaskPanic,
    Pending {
        rx: oneshot::Receiver<R>,
        is_executor_aborted: Arc<AtomicBool>
    }
}

impl<R> Future for TaskHandle<R> {
    type Output = Result<R, TaskError>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {

        use std::task::Poll;

        match &mut self.state {
            TaskHandleState::PrevTaskPanic => {
                Poll::Ready(Err(TaskError { kind: TaskErrorKind::Panic }))
            },
            TaskHandleState::Pending { rx, is_executor_aborted } => {
                match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
                    Poll::Ready(Err(_)) => {
                        if is_executor_aborted.load(Ordering::Acquire) {
                            Poll::Ready(Err(TaskError { kind: TaskErrorKind::Cancelled }))
                        }
                        else {
                            Poll::Ready(Err(TaskError { kind: TaskErrorKind::Panic }))
                        }
                    },
                    Poll::Pending => Poll::Pending,
                }
            },
        }
    }
}

/// Error that occurred while waiting for a task to complete.
#[derive(Debug)]
pub struct TaskError {
    kind: TaskErrorKind
}

#[derive(Debug)]
enum TaskErrorKind {
    Cancelled,
    Panic,
}

impl TaskError {

    /// Returns `true` if the error was caused by the [`Executor`] was dropped before the task completes.
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
    /// let handle = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(10)).await;
    /// }));
    /// 
    /// drop(executor);
    /// 
    /// let err = handle.await.unwrap_err();
    /// assert!(err.is_cancelled());
    /// assert!(!err.is_panic());
    /// # });
    /// # }
    /// ```
    pub fn is_cancelled(&self) -> bool {
        matches!(self.kind, TaskErrorKind::Cancelled)
    }

    /// Returns `true` if the error was caused by the task or any previous task panicking.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// let handle1 = executor.spawn(move |_| Box::pin(async move {
    ///     panic!()
    /// }));
    /// let handle2 = executor.spawn(move |_| Box::pin(async move {
    ///     
    /// }));
    /// 
    /// let err1 = handle1.await.unwrap_err();
    /// assert!(err1.is_panic());
    /// assert!(!err1.is_cancelled());
    /// 
    /// // Subsequent tasks also panic
    /// // because the state invariants may have been violated
    /// // by the preceding task's panic.
    /// let err2 = handle2.await.unwrap_err();
    /// assert!(err2.is_panic());
    /// assert!(!err2.is_cancelled());
    /// # });
    /// # }
    /// ```
    pub fn is_panic(&self) -> bool {
        matches!(self.kind, TaskErrorKind::Panic)
    }
}

impl fmt::Display for TaskError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            TaskErrorKind::Cancelled => write!(f, "task was cancelled"),
            TaskErrorKind::Panic => write!(f, "task panicked"),
        }
    }
}

impl std::error::Error for TaskError {}

impl From<TaskError> for std::io::Error {

    fn from(value: TaskError) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            match value.kind {
                TaskErrorKind::Cancelled => "task was cancelled",
                TaskErrorKind::Panic => "task panicked",
            },
        )
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn readme_example() {
        let executor = Executor::new(Vec::new());

        executor.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
            state.push(identity(0).await);
        }));

        executor.spawn_blocking(move |state| {
            state.push(1);
        });

        let task_result = executor.execute(move |state| Box::pin(async move {
            state.push(identity(2).await);
            "hello"
        })).await;
        assert_eq!(task_result, "hello");

        let task_result = executor.execute_blocking(move |state| {
            state.push(3);
            "world"
        }).await;
        assert_eq!(task_result, "world");

        let result = executor.join().await;
        assert_eq!(result, vec![0, 1, 2, 3]);

        async fn identity(v: u64) -> u64 {
            v
        }
    }

    #[tokio::test]
    async fn test1() {
        let executor = Executor::new(Vec::new());
        let c = 1000;

        for i in 0..c {
            if i % 2 == 0 {
                executor.spawn(move |state| Box::pin(async move { state.push(i); }));
            }
            else {
                executor.spawn_blocking(move |state| { state.push(i); });
            }
        }

        assert_eq!(executor.join().await, (0..c).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test2() {
        let executor = Executor::new(Vec::new());
        let c = 1000;

        for i in 0..c {
            if i % 2 == 0 {
                let r = executor.spawn(move |state| Box::pin(async move { 
                    state.push(i); 
                    i
                })).await;

                assert_eq!(r.unwrap(), i);
            }
            else {
                let r = executor.spawn_blocking(move |state| { 
                    state.push(i); 
                    i
                }).await;

                assert_eq!(r.unwrap(), i);
            }
        }

        assert_eq!(executor.join().await, (0..c).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test3() {
        let executor = Executor::new(Vec::new());
        let c = 1000;

        for i in 0..c {
            if i % 2 == 0 {
                executor.execute(move |state| Box::pin(async move { state.push(i); })).await;
            }
            else {
                executor.execute_blocking(move |state| { state.push(i); }).await;
            }
        }

        assert_eq!(executor.join().await, (0..c).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test4() {
        let executor = Executor::new(vec![0]);
        assert_eq!(executor.join().await, vec![0]);
    }

    #[tokio::test]
    async fn test5() {
        let executor = Executor::new(vec![0]);
        assert_eq!(executor.try_join().await.unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn test6() {
        let executor = Executor::new(vec![0]);
        executor.spawn(|_| Box::pin(async { panic!() }));
        let r = executor.try_join().await;
        assert!(r.as_ref().is_err_and(|e| e.is_panic()));
        assert!(r.as_ref().is_err_and(|e| !e.is_cancelled()));
    }

    #[tokio::test]
    async fn test7() {
        {
            let executor = Executor::new(());
        
            let (tx, rx) = oneshot::channel();
            let handle = executor.spawn(move |_| Box::pin(async {
                rx.await.unwrap();
            }));

            drop(executor);
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
        {
            let executor = Executor::new(());
        
            let (tx, rx) = oneshot::channel();
            let handle = executor.spawn_blocking(move |_| {
                rx.blocking_recv().unwrap();
            });

            drop(executor);
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
    }

    #[tokio::test]
    async fn test8() {
        {
            let executor = Executor::new(());
        
            let handle = executor.spawn(move |_| Box::pin(async {
                panic!()
            }));

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| !e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.is_panic()));
        }
        {
            let executor = Executor::new(());
        
            let handle = executor.spawn_blocking(move |_| {
                panic!()
            });

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| !e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.is_panic()));
        }
    }

    #[tokio::test]
    async fn test9() {
        {
            let executor = Executor::new(());
        
            let (tx, rx) = oneshot::channel();
            let handle = executor.spawn(move |_| Box::pin(async {
                rx.await.unwrap();
                panic!()
            }));

            drop(executor);
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
        {
            let executor = Executor::new(());
        
            let (tx, rx) = oneshot::channel();
            let handle = executor.spawn_blocking(move |_| {
                rx.blocking_recv().unwrap();
                panic!()
            });

            drop(executor);
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
    }

    #[tokio::test]
    async fn test10() {
        {
            let executor = Executor::new(());
        
            executor.spawn(move |_| Box::pin(async {
                panic!()
            }));
            let handle = executor.spawn(move |_| Box::pin(async {
                
            }));

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| !e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.is_panic()));
        }
        {
            let executor = Executor::new(());
        
            executor.spawn_blocking(move |_| {
                panic!()
            });
            let handle = executor.spawn_blocking(move |_| {
                
            });

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| !e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.is_panic()));
        }
    }
}

#[cfg(test)]
mod tests2 {
    use super::*;

    #[tokio::test]
    async fn test1() {
        let se = Executor::new("0".to_string());

        se.spawn_blocking(|ctx| {
            if ctx == "0" {
                *ctx = "1".into();
            }
        }).await.ok();
        se.spawn(|ctx| Box::pin(async move {
            if ctx == "1" {
                *ctx = "2".into();
            }
        })).await.ok();
        se.spawn(|ctx| Box::pin(async move {
            if ctx == "2" {
                *ctx = "3".into();
            }
        })).await.ok();

        let result = se.join().await;

        assert_eq!(&result, "3")
    }

    #[tokio::test]
    async fn test2() {
        let se = Executor::new(0);

        tokio::join!(
            se.execute(|ctx| Box::pin(async{*ctx += 1;})),
            se.execute(|ctx| Box::pin(async{*ctx += 1;})),
            se.execute(|ctx| Box::pin(async{*ctx += 1;})),
            se.execute_blocking(|ctx| {*ctx += 1;}),
            se.execute_blocking(|ctx| {*ctx += 1;}),
            se.execute_blocking(|ctx| {*ctx += 1;}),
            se.execute_blocking(|ctx| {*ctx += 1;}),
            se.execute_blocking(|ctx| {*ctx += 1;}),
            se.execute_blocking(|ctx| {*ctx += 1;}),
        );

        let result = se.join().await;

        assert_eq!(result, 9)
    }

    #[tokio::test]
    async fn test3() {
        let se = Arc::new(Executor::new(0));

        let mut set = tokio::task::JoinSet::new();
        let i = 10000;
        for _ in 0..i {
            let se1 = Arc::clone(&se);
            let se2 = Arc::clone(&se);
            set.spawn(async move {
                se1.execute(|ctx| Box::pin(async{*ctx += 1;})).await;
            });
            set.spawn(async move {
                se2.execute_blocking(|ctx| {*ctx += 1;}).await;
            });
        }

        set.join_all().await;

        let result = Arc::into_inner(se).unwrap().join().await;

        assert_eq!(result, i * 2)
    }

    #[tokio::test]
    async fn test4() {
        let se = Executor::new(0);
        let result = se.join().await;
        assert_eq!(result, 0)
    }

    #[tokio::test]
    async fn test5() {
        let se = Executor::new(Vec::<&'static str>::new());

        tokio::join!(
            se.execute(|ctx| Box::pin(async {
                ctx.push("1");
                tokio::task::yield_now().await;
                ctx.push("2");
            })),
            se.execute(|ctx| Box::pin(async {
                ctx.push("3");
            })),
        );

        let result = se.join().await;
        assert_eq!(result, vec!["1", "2", "3"]);
    }

    #[tokio::test]
    #[should_panic]
    async fn test6() {
        let se = Executor::new(Vec::<&'static str>::new());

        se.execute(|_ctx| Box::pin(async {
            panic!()
        })).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test7() {
        let se = Executor::new(Vec::<&'static str>::new());

        se.execute_blocking(|_ctx| {
            panic!()
        }).await;
    }

    #[tokio::test]
    async fn test8() {
        let se = Executor::new(Vec::<&'static str>::new());

        // TaskHandle を待機しないと task で panic しても sumit_blocking は panic しない
        se.spawn_blocking(|_ctx| {
            panic!()
        });
    }

    #[tokio::test]
    async fn test9() {
        let se = Executor::new(Vec::<&'static str>::new());

        // TaskHandle を待機しないと task で panic しても sumit は panic しない
        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));
    }

    #[tokio::test]
    #[should_panic]
    async fn test10() {
        let se = Executor::new(Vec::<&'static str>::new());

        se.spawn_blocking(|_ctx| {
            panic!()
        }).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test11() {
        let se = Executor::new(Vec::<&'static str>::new());

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        })).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test12() {
        let se = Executor::new(Vec::<&'static str>::new());

        // task が panic　した場合、その後の task も panic　になる。

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));

        se.spawn_blocking(|_ctx| {}).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test13() {
        let se = Executor::new(Vec::<&'static str>::new());

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));

        se.join().await;
    }

    #[tokio::test]
    async fn test14() {
        let se = Executor::new(Vec::<u64>::new());
        let i = 1000;

        for i in 0..i {
            se.spawn(move |ctx| Box::pin(async move {
                ctx.push(i);
            }));
        }

        assert_eq!(se.join().await, (0..i).collect::<Vec<_>>())
    }

    #[tokio::test]
    async fn test15() {
        let se = Executor::new(Vec::<u64>::new());
        let s = tokio::sync::Mutex::new(0);

        se.spawn(move |ctx| Box::pin(async move {
            ctx.push({
                let mut s = s.lock().await;
                *s += 1;
                *s
            });
            ctx.push({
                let mut s = s.lock().await;
                *s += 1;
                *s
            });
            ctx.push({
                let mut s = s.lock().await;
                *s += 1;
                *s
            });
        }));

        assert_eq!(se.join().await, vec![1, 2, 3])
    }

    #[tokio::test]
    #[should_panic]
    async fn test16() {
        let se = Executor::new(());
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = se.spawn(|_| Box::pin(async {
            let _ = rx.await;
        }));
        drop(se);
        let _ = tx.send(());
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test17() {
        let executor = Executor::new(Vec::<u64>::new());

        let handle = executor.spawn(|state| Box::pin(async move {
            state.push(1);
        }));

        drop(handle);

        let result = executor.join().await;

        assert_eq!(result, vec![1]);
    }

    #[tokio::test]
    async fn test18() {
        let executor = Executor::new(Vec::<&'static str>::new());

        executor.spawn(|state| Box::pin(async move {
            state.push("async-1");
            tokio::task::yield_now().await;
            state.push("async-2");
        }));

        executor.spawn_blocking(|state| {
            state.push("blocking");
        });

        executor.spawn(|state| Box::pin(async move {
            state.push("async-3");
        }));

        assert_eq!(
            executor.join().await,
            vec![
                "async-1",
                "async-2",
                "blocking",
                "async-3",
            ]
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn test19() {
        let executor = Executor::new(());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let handle = executor.spawn(move |_| Box::pin(async move {
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));

        started_rx.await.unwrap();

        drop(executor);

        handle.await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test20() {
        let executor = Executor::new(Vec::<u64>::new());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        executor.spawn(move |state| Box::pin(async move {
            state.push(1);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));

        started_rx.await.unwrap();

        let queued_handle = executor.spawn(|state| Box::pin(async move {
            state.push(2);
        }));

        drop(executor);

        queued_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test21() {
        let executor = Executor::new(Vec::<u64>::new());

        for i in 0..1000 {
            executor.spawn(move |state| Box::pin(async move {
                state.push(i);
            }));
        }

        let result = executor.join().await;

        assert_eq!(result, (0..1000).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test22() {
        let se = Executor::new(0);
        let c = 4;

        for _ in 0..c {
            se.spawn(|s| Box::pin(async {
                *s += 1;
                tokio::time::sleep(std::time::Duration::from_secs(1))
            }));  
        }

        let result = se.join().await;
        assert_eq!(result, c)
    }
}