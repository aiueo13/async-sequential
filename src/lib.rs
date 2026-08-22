mod internal;
mod executor;
mod task_canceller;
mod task_error;
mod task_handle;

use internal::*;

pub use executor::Executor;
pub use task_canceller::TaskCanceller;
pub use task_error::TaskError;
pub use task_handle::TaskHandle;


#[cfg(test)]
mod tests3 {
    use std::{future::pending, sync::Arc, time::Duration};
    use super::*;
    use tokio::{sync::{mpsc, oneshot}, time::sleep};


    #[tokio::test]
    async fn test_completed() {
        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();

        let handle = executor.spawn(|_| Box::pin(async {
            rx.await.unwrap();
            42
        }));

        tx.send(()).unwrap();
        assert_eq!(handle.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_finished_after_completion() {
        let executor = Executor::new(());

        let handle = executor.spawn(|_| Box::pin(async {
            42
        }));

        executor.join().await;
        assert_eq!(handle.await.unwrap(), 42);
    }


    #[tokio::test]
    async fn test_cancel_queued_task() {
        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();

        let running = executor.spawn(|_| Box::pin(async {
            rx.await.unwrap();
        }));
        let handle = executor.spawn(|_| Box::pin(async {
            42
        }));

        assert!(handle.cancel());
        assert!(handle.await.unwrap_err().is_cancelled());

        // The running task must not be affected by cancellation of the queued task.
        tx.send(()).unwrap();
        assert!(running.await.is_ok());
    }


    #[tokio::test]
    async fn test_cancel_running_task_returns_false() {
        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let handle = executor.spawn(|_| Box::pin(async {
            tx2.send(()).unwrap();
            rx.await.unwrap();
            42
        }));

        rx2.await.unwrap();
        assert!(!handle.cancel());

        tx.send(()).unwrap();
        assert_eq!(handle.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_cancel_twice() {
        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();

        let running = executor.spawn(|_| Box::pin(async {
            rx.await.unwrap();
        }));
        let handle = executor.spawn(|_| Box::pin(async {
            42
        }));

        assert!(handle.cancel());
        assert!(!handle.cancel());

        tx.send(()).unwrap();
        assert!(handle.await.unwrap_err().is_cancelled());
        assert!(running.await.is_ok());
    }


    #[tokio::test]
    async fn test_executor_drop() {
        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let handle = executor.spawn(|_| Box::pin(async {
            tx2.send(()).unwrap();
            rx.await.unwrap();
            42
        }));

        rx2.await.unwrap();

        drop(executor);

        assert!(!handle.cancel());

        tx.send(()).unwrap();
        assert!(handle.await.unwrap_err().is_cancelled());
    }


    #[tokio::test]
    async fn test_task_panic() {
        let executor = Executor::new(());

        let handle = executor.spawn(|_| Box::pin(async {
            panic!("task panic");
        }));

        assert!(handle.await.unwrap_err().is_panic());
    }


    #[tokio::test]
    async fn test_prev_task_panic() {
        let executor = Executor::new(());

        let first = executor.spawn(|_| Box::pin(async {
            panic!("first task panic");
        }));

        assert!(first.await.is_err());

        let second = executor.spawn(|_| Box::pin(async {
            42
        }));

        assert!(!second.cancel());
        assert!(second.await.unwrap_err().is_panic());
    }


    #[tokio::test]
    async fn test_is_finished_while_running() {
        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let handle = executor.spawn(|_| Box::pin(async {
            tx2.send(()).unwrap();
            rx.await.unwrap();
        }));

        rx2.await.unwrap();
        tx.send(()).unwrap();
        assert!(handle.await.is_ok());
    }

    #[tokio::test]
    async fn test_cancel_queued_task_does_not_run() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();

        let running = executor.spawn(|_| Box::pin(async {
            rx.await.unwrap();
        }));

        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();

        let handle = executor.spawn(move |_| Box::pin(async move {
            executed_clone.store(true, Ordering::SeqCst);
        }));

        assert!(handle.cancel());
        assert!(handle.await.unwrap_err().is_cancelled());

        tx.send(()).unwrap();
        running.await.unwrap();

        assert!(!executed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_panic_before_executor_drop() {
        let executor = Executor::new(());

        let handle = executor.spawn(|_| Box::pin(async {
            panic!()
        }));

        let _ = executor.spawn(|_| Box::pin(async {})).await;

        drop(executor);
        let err = handle.await.unwrap_err();
        assert!(err.is_panic());
        assert!(!err.is_cancelled());
    }

    #[tokio::test]
    async fn test_panic_after_executor_drop() {
        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();

        let handle = executor.spawn(|_| Box::pin(async {
            rx.await.unwrap();
            panic!()
        }));

        drop(executor);
        tx.send(()).unwrap();
        let err = handle.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
    }

    #[tokio::test]
    async fn test_resolve_task_handle_after_cancel() {
        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();
    
        executor.spawn(move |_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));

        let handle = executor.spawn(move |_| Box::pin(async {}));

        rx.await.unwrap();
        handle.cancel();
        let err = handle.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(!err.is_panic());
    }

    #[tokio::test]
    async fn test_resolve_task_handle_after_executor_aborted() {
        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();

        let handle1 = executor.spawn(move |_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let handle2 = executor.spawn(move |_| Box::pin(async {}));

        rx.await.unwrap();
        drop(executor);

        let err1 = handle1.await.unwrap_err();
        assert!(err1.is_cancelled());
        assert!(!err1.is_panic());
        let err2 = handle2.await.unwrap_err();
        assert!(err2.is_cancelled());
        assert!(!err2.is_panic());
    }

    #[tokio::test]
    async fn test_resolve_task_handle_after_executor_canncelled() {
        let executor = Executor::new(Vec::new());
        let (tx, rx) = oneshot::channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();

        let handle1 = executor.spawn(move |state| Box::pin(async move {
            tx.send(()).unwrap();
            while let Some(v) = rx2.recv().await {
                state.push(v);
            }
            state.clone()
        }));
        let handle2 = executor.spawn(move |_| Box::pin(async {
            pending::<()>().await;
        }));

        tx2.send(0).unwrap();
        rx.await.unwrap();
        
        executor.cancel();

        let err2 = handle2.await.unwrap_err();
        assert!(err2.is_cancelled());
        assert!(!err2.is_panic());

        tx2.send(1).unwrap();
        drop(tx2);

        assert_eq!(handle1.await.unwrap(), vec![0, 1]);
    }

    #[tokio::test]
    async fn test_cancel_join() {
        let executor = Executor::new(());
        let (tx, rx) = oneshot::channel();

        let running = executor.spawn(move |_| Box::pin(async move {
            tx.send(()).unwrap();
            sleep(Duration::from_millis(500)).await;
            "complete"
        }));

        let pending = executor.spawn(move |_| Box::pin(async move { }));

        rx.await.unwrap();
        executor.cancel_and_join().await;
        assert_eq!(running.await.unwrap(), "complete");
        assert!(pending.await.unwrap_err().is_cancelled());
    }
}

#[cfg(test)]
mod tests2 {
    use tokio::sync::oneshot;
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
mod tests {
    use std::sync::Arc;
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