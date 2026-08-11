use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use tokio::{task::{spawn, spawn_blocking}, sync::{oneshot, mpsc}};


/// Executor for running asynchronous and blocking tasks sequentially on a shared mutable state.
/// 
/// # Example
/// ```
/// # fn main() {
/// # tokio_test::block_on(async {
/// let executor = async_sequential::StatefulExecutor::new(0);
///
/// executor.execute(move |state| Box::pin(async move {
///     *state += 1;
/// })).await;
/// 
/// executor.execute_blocking(move |state| {
///     *state += 10;
/// }).await;
///
/// assert_eq!(executor.join().await, 11);
/// # });
/// # }
/// ```
pub struct StatefulExecutor<S> {
    executor: Arc<std::sync::Mutex<ExecutorState<S>>>,
}

enum ExecutorState<S> {
    Running {
        tx: mpsc::UnboundedSender<Task<S>>,
        done_rx: Option<oneshot::Receiver<()>>
    },
    Pending {
        tx: mpsc::UnboundedSender<Task<S>>,
        rx: mpsc::UnboundedReceiver<Task<S>>,
        state: S
    },
    Closed,
}

impl<S> StatefulExecutor<S> {

    /// Creates a new executor with the given initial state.
    ///
    /// The state is owned by the executor
    /// and is made available to tasks through exclusive mutable access.
    pub fn new(state: S) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<Task<S>>();
        Self {
            executor: Arc::new(std::sync::Mutex::new(ExecutorState::Pending { 
                rx, 
                tx, 
                state
            })),
        }
    }
}

impl<S: Send + 'static> StatefulExecutor<S> {
    
    /// Queues an asynchronous task for sequential execution and waits for it to complete.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued, regardless of whether they are asynchronous or blocking.
    /// While the task is running, it has exclusive mutable access to the executor's state.
    ///
    /// # Panics
    /// Panics if the task panics or if a previous task panicked.
    ///
    /// # Example
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::StatefulExecutor::new(0);
    ///
    /// executor.execute(move |state| Box::pin(async move {
    ///     *state += 1;
    /// })).await;
    /// 
    /// executor.execute(move |state| Box::pin(async move {
    ///     *state += 10;
    /// })).await;
    /// 
    /// // This task will not be executed because it is never awaited
    /// executor.execute(move |state| Box::pin(async move {
    ///     *state += 100;
    /// }))/*.await*/;
    /// 
    /// assert_eq!(executor.join().await, 11);
    /// # });
    /// # }
    /// ```
    pub async fn execute<T, R>(&self, task: T) -> R
    where
        T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
        R: Send + 'static,
    {
        self.submit(task).await
    }

    /// Queues a blocking task for sequential execution and waits for it to complete.
    ///
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued, regardless of whether they are asynchronous or blocking.
    /// While the task is running, it has exclusive mutable access to the executor's state.
    ///
    /// The blocking task is executed on a blocking thread 
    /// so that it does not block the asynchronous runtime.
    /// 
    /// # Panics
    /// Panics if the task panics or if a previous task panicked.
    ///
    /// # Example
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::StatefulExecutor::new(0);
    ///
    /// executor.execute_blocking(move |state| {
    ///     *state += 1;
    /// }).await;
    /// 
    /// executor.execute_blocking(move |state| {
    ///     *state += 10;
    /// }).await;
    /// 
    /// // This task will not be executed because it is never awaited
    /// executor.execute_blocking(move |state| {
    ///     *state += 100;
    /// })/*.await*/;
    ///
    /// assert_eq!(executor.join().await, 11);
    /// # });
    /// # }
    /// ```
    pub async fn execute_blocking<T, R>(&self, task: T) -> R
    where
        T: (FnOnce(&mut S) -> R) + Send + 'static,
        R: Send + 'static,
    {
        self.submit_blocking(task).await
    }

    /// Queues an asynchronous task for sequential execution, 
    /// returning a [`TaskHandle`] to wait for it to complete.
    ///
    /// The task is executed even if the returned [`TaskHandle`] is dropped or never awaited.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued, regardless of whether they are asynchronous or blocking.
    /// While the task is running, it has exclusive mutable access to the executor's state.
    /// 
    /// # Panics
    /// Panics when the [`TaskHandle`] is awaited if the task panics or if a previous task panicked.
    ///
    /// # Example
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::StatefulExecutor::new(Vec::<&str>::new());
    ///
    /// executor.execute(move |state| Box::pin(async move {
    ///     state.push("first");
    /// })).await;
    /// 
    /// executor.submit(move |state| Box::pin(async move {
    ///     state.push("second");
    /// }));
    /// 
    /// executor.submit(move |state| Box::pin(async move {
    ///     state.push("third");
    /// }));
    /// 
    /// // This task will not be executed because it is never awaited
    /// executor.execute(move |state| Box::pin(async move {
    ///     state.push("never");
    /// }))/*.await*/;
    /// 
    /// executor.execute(move |state| Box::pin(async move {
    ///     state.push("fourth");
    /// })).await;
    /// 
    /// assert_eq!(
    ///     executor.join().await, 
    ///     vec!["first", "second", "third", "fourth"]
    /// );
    /// # });
    /// # }
    /// ```
    pub fn submit<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let task = Task::Async(Box::new(|s: &mut S| 
            Box::pin(async {
                let res = task(s).await;
                let _ = tx.send(res);
            })
        ));
        self.submit_inner(task);
        TaskHandle { rx }
    }

    /// Queues a blocking task for sequential execution, 
    /// returning a [`TaskHandle`] to wait for it to complete.
    ///
    /// The task is executed even if the returned [`TaskHandle`] is dropped or never awaited.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued, regardless of whether they are asynchronous or blocking.
    /// While the task is running, it has exclusive mutable access to the executor's state.
    /// 
    /// The blocking task is executed on a blocking thread 
    /// so that it does not block the asynchronous runtime.
    /// 
    /// # Panics
    /// Panics when the [`TaskHandle`] is awaited if the task panics or if a previous task panicked.
    ///
    /// # Example
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// let executor = async_sequential::StatefulExecutor::new(Vec::<&str>::new());
    ///
    /// executor.execute_blocking(move |state| {
    ///     state.push("first");
    /// }).await;
    /// 
    /// executor.submit_blocking(move |state| {
    ///     state.push("second");
    /// });
    /// 
    /// executor.submit_blocking(move |state| {
    ///     state.push("third");
    /// });
    /// 
    /// // This task will not be executed because it is never awaited
    /// executor.execute_blocking(move |state| {
    ///     state.push("never");
    /// })/*.await*/;
    /// 
    /// executor.execute_blocking(move |state| {
    ///     state.push("fourth");
    /// }).await;
    /// 
    /// assert_eq!(
    ///     executor.join().await, 
    ///     vec!["first", "second", "third", "fourth"]
    /// );
    /// # });
    /// # }
    /// ```
    pub fn submit_blocking<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: (FnOnce(&mut S) -> R) + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let task = Task::Blocking(Box::new(move |s: &mut S| {
            let res = task(s);
            let _ = tx.send(res);
        }));
        self.submit_inner(task);
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
    /// let executor = async_sequential::StatefulExecutor::new(0);
    ///
    /// executor.execute(move |state| Box::pin(async move {
    ///     *state += 1;
    /// })).await;
    /// 
    /// executor.execute_blocking(move |state| {
    ///     *state += 10;
    /// }).await;
    ///
    /// assert_eq!(executor.join().await, 11);
    /// # });
    /// # }
    /// ```
    pub async fn join(self) -> S {
        {  
            let mut locked_executor = self.executor.lock().unwrap();
            let is_pending = match *locked_executor {
                ExecutorState::Pending { .. } => true,
                _ => false
            };
            if is_pending {
                return match std::mem::replace(&mut *locked_executor, ExecutorState::Closed) {
                    ExecutorState::Pending { state, .. } => state,
                    _ => unreachable!()
                }
            }
        }

        self.execute(|_| Box::pin(async {})).await;

        let done_rx = match &mut *self.executor.lock().unwrap() {
            ExecutorState::Pending { .. } => None,
            ExecutorState::Running { done_rx, .. } => { done_rx.take() },
            ExecutorState::Closed => None,
        };

        if let Some(done_rx) = done_rx {
            let _ = done_rx.await;
        }

        match std::mem::replace(&mut *self.executor.lock().unwrap(), ExecutorState::Closed) {
            ExecutorState::Pending { state, .. } => state,
            ExecutorState::Running { .. } => panic!("worker is still running"),
            ExecutorState::Closed => panic!("executor has already been closed"),
        }
    }

    fn submit_inner(&self, task: Task<S>) {
        let tx = match *self.executor.lock().unwrap() {
            ExecutorState::Closed => panic!("illegal closed state"),
            ExecutorState::Running { ref tx, .. } => tx.clone(),
            ExecutorState::Pending { ref tx, .. } => tx.clone(),
        };

        tx.send(task).expect("rx has been shut down");

        let (done_tx, done_rx) = oneshot::channel();
        let (tx, mut rx, mut state) = match std::mem::replace(
            &mut *self.executor.lock().unwrap(), 
            ExecutorState::Running { tx, done_rx: Some(done_rx) }
        ) {
            ExecutorState::Closed => panic!("illegal closed state"),
            ExecutorState::Running { .. } => return,
            ExecutorState::Pending { tx, rx, state } => (tx, rx, state),
        };

        let executor = Arc::clone(&self.executor);
        spawn(async move {
            let mut job = rx.recv().await.unwrap();
           
            loop {
                match job {
                    Task::Blocking(job) => {
                        state = spawn_blocking(move || {
                            job(&mut state);
                            state
                        }).await.expect("blocking task unexpectedly failed");
                    },
                    Task::Async(job) => job(&mut state).await,
                }

                let mut locked_executor = executor.lock().unwrap();
                if let Ok(next_f) = rx.try_recv() {
                    job = next_f;
                }
                else {
                    *locked_executor = ExecutorState::Pending { tx, rx, state };
                    break;
                }
            }

            let _ = done_tx.send(());
        });
    }
}

enum Task<S> {
    Blocking(Box<dyn FnOnce(&mut S) + Send>),
    Async(Box<dyn for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> + Send>),
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
            std::task::Poll::Ready(Err(_)) => panic!("task panicked or executor was shut down"),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;


    #[tokio::test]
    async fn test1() {
        let se = StatefulExecutor::new("0".to_string());

        se.execute_blocking(|ctx| {
            if ctx == "0" {
                *ctx = "1".into();
            }
        }).await;
        se.execute(|ctx| Box::pin(async move {
            if ctx == "1" {
                *ctx = "2".into();
            }
        })).await;
        se.execute(|ctx| Box::pin(async move {
            if ctx == "2" {
                *ctx = "3".into();
            }
        })).await;

        let result = se.join().await;

        assert_eq!(&result, "3")
    }

    #[tokio::test]
    async fn test2() {
        let se = StatefulExecutor::new(0);

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
        let se = Arc::new(StatefulExecutor::new(0));

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
        let se = StatefulExecutor::new(0);
        let result = se.join().await;
        assert_eq!(result, 0)
    }

    #[tokio::test]
    async fn test5() {
        let se = StatefulExecutor::new(Vec::<&'static str>::new());

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
        let se = StatefulExecutor::new(Vec::<&'static str>::new());

        se.execute(|_ctx| Box::pin(async {
            panic!()
        })).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test7() {
        let se = StatefulExecutor::new(Vec::<&'static str>::new());

        se.execute_blocking(|_ctx| {
            panic!()
        }).await;
    }

    #[tokio::test]
    async fn test8() {
        let se = StatefulExecutor::new(Vec::<&'static str>::new());

        // TaskHandle を待機しないと task で panic しても sumit_blocking は panic しない
        se.submit_blocking(|_ctx| {
            panic!()
        });
    }

    #[tokio::test]
    async fn test9() {
        let se = StatefulExecutor::new(Vec::<&'static str>::new());

        // TaskHandle を待機しないと task で panic しても sumit は panic しない
        se.submit(|_ctx| Box::pin(async {
            panic!()
        }));
    }

    #[tokio::test]
    #[should_panic]
    async fn test10() {
        let se = StatefulExecutor::new(Vec::<&'static str>::new());

        se.submit_blocking(|_ctx| {
            panic!()
        }).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test11() {
        let se = StatefulExecutor::new(Vec::<&'static str>::new());

        se.submit(|_ctx| Box::pin(async {
            panic!()
        })).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test12() {
        let se = StatefulExecutor::new(Vec::<&'static str>::new());

        // task が panic　した場合、その後の task も panic　になる。

        se.submit(|_ctx| Box::pin(async {
            panic!()
        }));

        se.execute_blocking(|_ctx| {}).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test13() {
        let se = StatefulExecutor::new(Vec::<&'static str>::new());

        se.submit(|_ctx| Box::pin(async {
            panic!()
        }));

        se.join().await;
    }

    #[tokio::test]
    async fn test14() {
        let se = StatefulExecutor::new(Vec::<u64>::new());
        let i = 1000;

        for i in 0..i {
            se.submit(move |ctx| Box::pin(async move {
                ctx.push(i);
            }));
        }

        assert_eq!(se.join().await, (0..i).collect::<Vec<_>>())
    }

    #[tokio::test]
    async fn test15() {
        let se = StatefulExecutor::new(Vec::<u64>::new());
        let s = tokio::sync::Mutex::new(0);

        se.submit(move |ctx| Box::pin(async move {
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
}