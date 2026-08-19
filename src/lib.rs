use std::pin::Pin;
use std::future::Future;
use std::sync::Mutex as SyncMutex;
use tokio::{task::{spawn, spawn_blocking, JoinHandle as SpawnJoinHandle}, sync::{oneshot, mpsc}};


/// Executor for running asynchronous and blocking tasks sequentially on a shared mutable state.
/// 
/// When the [`Executor`] is dropped, all queued tasks and running asynchronous tasks are aborted.
/// Blocking tasks that have already started may continue running to completion.
/// 
/// # Example
/// ```
/// # fn main() {
/// # tokio_test::block_on(async {
/// let executor = async_sequential::Executor::new(Vec::<&str>::new());
///
/// executor.spawn(move |state| Box::pin(async move {
///     state.push("first");
/// }));
/// 
/// executor.spawn_blocking(move |state| {
///     state.push("second");
/// });
/// 
/// let task_result = executor.spawn(move |state| Box::pin(async move {
///     state.push("third");
///     "hello world"
/// })).await;
/// assert_eq!(task_result, "hello world");
/// 
/// let result = executor.join().await;
/// assert_eq!(result, vec!["first", "second", "third"]);
/// # });
/// # }
/// ```
pub struct Executor<S> {
    executor: SyncMutex<Option<ExecutorState<S>>>,
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
        }
    }
}

impl<S: Default> Default for Executor<S> {

    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S: Send + 'static> Executor<S> {

    /// Queues an asynchronous task for sequential execution, 
    /// returning a [`TaskHandle`] to wait for it to complete.
    ///
    /// The task is executed even if the returned [`TaskHandle`] is dropped or never awaited.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued, regardless of whether they are asynchronous or blocking.
    /// 
    /// When the [`Executor`] is dropped, all queued tasks and running asynchronous tasks are aborted.
    /// Blocking tasks that have already started may continue running to completion.
    /// 
    /// # Panics
    /// Awaiting the [`TaskHandle`] panics in the following cases:
    /// - The task panics.
    /// - A previous task panicked.
    /// - The [`Executor`] is dropped.
    /// 
    /// # Example
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(Vec::<&str>::new());
    ///
    /// executor.spawn(move |state| Box::pin(async move {
    ///     state.push("first");
    /// }));
    /// 
    /// executor.spawn_blocking(move |state| {
    ///     state.push("second");
    /// });
    /// 
    /// let task_result = executor.spawn(move |state| Box::pin(async move {
    ///     state.push("third");
    ///     "hello world"
    /// })).await;
    /// assert_eq!(task_result, "hello world");
    /// 
    /// let result = executor.join().await;
    /// assert_eq!(result, vec!["first", "second", "third"]);
    /// # });
    /// # }
    /// ```
    pub fn spawn<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.submit(Task::Async(Box::new(|s: &mut S| Box::pin(async {
            let _ = tx.send(task(s).await);
        }))));
        TaskHandle { rx }
    }

    /// Queues a blocking task for sequential execution, 
    /// returning a [`TaskHandle`] to wait for it to complete.
    ///
    /// The task is executed even if the returned [`TaskHandle`] is dropped or never awaited.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued, regardless of whether they are asynchronous or blocking.
    /// 
    /// The blocking task is executed on a blocking thread
    /// to avoid blocking the asynchronous runtime.
    /// 
    /// When the [`Executor`] is dropped, all queued tasks and running asynchronous tasks are aborted.
    /// Blocking tasks that have already started may continue running to completion.
    /// 
    /// # Panics
    /// Awaiting the [`TaskHandle`] panics in the following cases:
    /// - The task panics.
    /// - A previous task panicked.
    /// - The [`Executor`] is dropped.
    /// 
    /// # Example
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(Vec::<&str>::new());
    ///
    /// executor.spawn(move |state| Box::pin(async move {
    ///     state.push("first");
    /// }));
    /// 
    /// executor.spawn_blocking(move |state| {
    ///     state.push("second");
    /// });
    /// 
    /// let task_result = executor.spawn(move |state| Box::pin(async move {
    ///     state.push("third");
    ///     "hello world"
    /// })).await;
    /// assert_eq!(task_result, "hello world");
    /// 
    /// let result = executor.join().await;
    /// assert_eq!(result, vec!["first", "second", "third"]);
    /// # });
    /// # }
    /// ```
    pub fn spawn_blocking<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: (FnOnce(&mut S) -> R) + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.submit(Task::Blocking(Box::new(move |s: &mut S| {
            let _ = tx.send(task(s));
        })));
        TaskHandle { rx }
    }

    /// Waits for all queued tasks to complete and returns the final state.
    ///
    /// # Panics
    /// Panics if any task panicked.
    /// 
    /// # Example
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::Executor::new(Vec::<&str>::new());
    ///
    /// executor.spawn(move |state| Box::pin(async move {
    ///     state.push("first");
    /// }));
    /// 
    /// executor.spawn_blocking(move |state| {
    ///     state.push("second");
    /// });
    /// 
    /// let result = executor.join().await;
    /// assert_eq!(result, vec!["first", "second"]);
    /// # });
    /// # }
    /// ```
    pub async fn join(self) -> S {
        match self.executor.lock().expect("any task has been panicked").take() {
            Some(ExecutorState::Idle { state }) => state,
            Some(ExecutorState::Running { handle, task_tx }) => {
                drop(task_tx);
                handle.await.expect("any task has been panicked")
            }
            None => unreachable!("illegal closed executor"),
        }
    }

    fn submit(&self, task: Task<S>) {
        let mut locked_executor = self.executor.lock().expect("any task has been panicked");

        if let Some(ExecutorState::Running { ref task_tx, .. }) = *locked_executor {
            task_tx.send(task).expect("any task has been panicked");
            return;
        }

        *locked_executor = {
            let Some(ExecutorState::Idle { mut state }) = locked_executor.take() else {
                unreachable!("illegal closed executor")
            };

            let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task<S>>();
            task_tx.send(task).expect("illegal closed task_rx");

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
    }
}

impl<S> Drop for Executor<S> {

    fn drop(&mut self) {
        match self.executor.lock().ok().and_then(|mut e| e.take()) {
            Some(ExecutorState::Idle { .. }) => {},
            Some(ExecutorState::Running { handle, .. }) => {
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

pub struct TaskHandle<R> {
    rx: oneshot::Receiver<R>,
}

impl<R> Future for TaskHandle<R> {
    type Output = R;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {

        match Pin::new(&mut self.rx).poll(cx) {
            std::task::Poll::Ready(Ok(value)) => std::task::Poll::Ready(value),
            std::task::Poll::Ready(Err(_)) => panic!("any task panicked or executor dropped"),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;


    #[tokio::test]
    async fn readme_example() {
        let executor = Executor::new(Vec::new());

        executor.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
            state.push(identity(0).await);
        }));

        executor.spawn_blocking(move |state: &mut Vec<u64>| {
            state.push(1);
        });

        let task_result = executor.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
            state.push(identity(2).await);
            "hello world"
        })).await;
        assert_eq!(task_result, "hello world");

        let result = executor.join().await;
        assert_eq!(result, vec![0, 1, 2]);

        async fn identity(v: u64) -> u64 {
            v
        }
    }

    #[tokio::test]
    async fn test1() {
        let se = Executor::new("0".to_string());

        se.spawn_blocking(|ctx| {
            if ctx == "0" {
                *ctx = "1".into();
            }
        }).await;
        se.spawn(|ctx| Box::pin(async move {
            if ctx == "1" {
                *ctx = "2".into();
            }
        })).await;
        se.spawn(|ctx| Box::pin(async move {
            if ctx == "2" {
                *ctx = "3".into();
            }
        })).await;

        let result = se.join().await;

        assert_eq!(&result, "3")
    }

    #[tokio::test]
    async fn test2() {
        let se = Executor::new(0);

        tokio::join!(
            se.spawn(|ctx| Box::pin(async{*ctx += 1;})),
            se.spawn(|ctx| Box::pin(async{*ctx += 1;})),
            se.spawn(|ctx| Box::pin(async{*ctx += 1;})),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
            se.spawn_blocking(|ctx| {*ctx += 1;}),
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
                se1.spawn(|ctx| Box::pin(async{*ctx += 1;})).await;
            });
            set.spawn(async move {
                se2.spawn_blocking(|ctx| {*ctx += 1;}).await;
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
            se.spawn(|ctx| Box::pin(async {
                ctx.push("1");
                tokio::task::yield_now().await;
                ctx.push("2");
            })),
            se.spawn(|ctx| Box::pin(async {
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

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        })).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test7() {
        let se = Executor::new(Vec::<&'static str>::new());

        se.spawn_blocking(|_ctx| {
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
        }).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test11() {
        let se = Executor::new(Vec::<&'static str>::new());

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        })).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test12() {
        let se = Executor::new(Vec::<&'static str>::new());

        // task が panic　した場合、その後の task も panic　になる。

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));

        se.spawn_blocking(|_ctx| {}).await;
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
        handle.await;
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

        handle.await;
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

        queued_handle.await;
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