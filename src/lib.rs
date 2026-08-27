mod internal;
mod task_queue;
mod task_queue_join_error;
mod task_canceller;
mod task_error;
mod task_handle;
mod task_spawner;

pub use task_queue::TaskQueue;
pub use task_queue_join_error::TaskQueueJoinError;
pub use task_spawner::TaskSpawner;
pub use task_canceller::TaskCanceller;
pub use task_error::{TaskError, TaskErrorKind};
pub use task_handle::TaskHandle;

#[cfg(test)]
mod test_readme {
    use crate as async_sequential;
    use std::{thread, time::Duration};
    use tokio::time::sleep;

    #[tokio::test]
    async fn test() {
        let queue = async_sequential::TaskQueue::new(Vec::new());

        // Tasks are executed in the order in which they are spawned.
        queue.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
            sleep(Duration::from_secs(1)).await;
            state.push(1);
        }));

        // A spawner can be used to spawn tasks from another thread or asynchronous task.
        let spawner = queue.spawner();
        tokio::spawn(async move {
            let task_handle1 = spawner.spawn_blocking(move |state| {
                thread::sleep(Duration::from_secs(2));
                state.push(2);
                "hello"
            });
            let task_handle2 = spawner.spawn(move |state| Box::pin(async move {
                sleep(Duration::from_secs(1)).await;
                state.push(3);
                "world"
            }));

            assert_eq!(task_handle1.await.unwrap(), "hello");
            assert_eq!(task_handle2.await.unwrap(), "world");

            drop(spawner);
        });

        // Wait for all tasks to complete.
        // NOTE: This does not complete as long as any spawner remains alive.
        let result = queue.join().await;
        assert_eq!(result, vec![1, 2, 3]);
    }
}

#[cfg(test)]
mod tests3 {
    use std::{future::pending, sync::Arc, time::Duration};
    use super::*;
    use tokio::{sync::{mpsc, oneshot}, time::sleep};


    #[tokio::test]
    async fn test_completed() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();

        let handle = queue.spawn(|_| Box::pin(async {
            rx.await.unwrap();
            42
        }));

        tx.send(()).unwrap();
        assert_eq!(handle.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_finished_after_completion() {
        let queue = TaskQueue::new(());

        let handle = queue.spawn(|_| Box::pin(async {
            42
        }));

        queue.join().await;
        assert_eq!(handle.await.unwrap(), 42);
    }


    #[tokio::test]
    async fn test_cancel_queued_task() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();

        let running = queue.spawn(|_| Box::pin(async {
            rx.await.unwrap();
        }));
        let handle = queue.spawn(|_| Box::pin(async {
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
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let handle = queue.spawn(|_| Box::pin(async {
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
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();

        let running = queue.spawn(|_| Box::pin(async {
            rx.await.unwrap();
        }));
        let handle = queue.spawn(|_| Box::pin(async {
            42
        }));

        assert!(handle.cancel());
        assert!(!handle.cancel());

        tx.send(()).unwrap();
        assert!(handle.await.unwrap_err().is_cancelled());
        assert!(running.await.is_ok());
    }


    #[tokio::test]
    async fn test_queue_drop() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let handle = queue.spawn(|_| Box::pin(async {
            tx2.send(()).unwrap();
            rx.await.unwrap();
            42
        }));

        rx2.await.unwrap();

        drop(queue);

        assert!(!handle.cancel());

        tx.send(()).unwrap();
        assert!(handle.await.unwrap_err().is_cancelled());
    }


    #[tokio::test]
    async fn test_task_panic() {
        let queue = TaskQueue::new(());

        let handle = queue.spawn(|_| Box::pin(async {
            panic!("task panic");
        }));

        assert!(handle.await.unwrap_err().is_panic());
    }


    #[tokio::test]
    async fn test_prev_task_panic() {
        let queue = TaskQueue::new(());

        let first = queue.spawn(|_| Box::pin(async {
            panic!("first task panic");
        }));

        assert!(first.await.is_err());

        let second = queue.spawn(|_| Box::pin(async {
            42
        }));

        assert!(!second.cancel());
        assert!(second.await.unwrap_err().is_panic());
    }


    #[tokio::test]
    async fn test_is_finished_while_running() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let handle = queue.spawn(|_| Box::pin(async {
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

        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();

        let running = queue.spawn(|_| Box::pin(async {
            rx.await.unwrap();
        }));

        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();

        let handle = queue.spawn(move |_| Box::pin(async move {
            executed_clone.store(true, Ordering::SeqCst);
        }));

        assert!(handle.cancel());
        assert!(handle.await.unwrap_err().is_cancelled());

        tx.send(()).unwrap();
        running.await.unwrap();

        assert!(!executed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_panic_before_queue_drop() {
        let queue = TaskQueue::new(());

        let handle = queue.spawn(|_| Box::pin(async {
            panic!()
        }));

        let _ = queue.spawn(|_| Box::pin(async {})).await;

        drop(queue);
        let err = handle.await.unwrap_err();
        assert!(err.is_panic());
        assert!(!err.is_cancelled());
    }

    #[tokio::test]
    async fn test_panic_after_queue_drop() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();

        let handle = queue.spawn(|_| Box::pin(async {
            rx.await.unwrap();
            panic!()
        }));

        drop(queue);
        tx.send(()).unwrap();
        let err = handle.await.unwrap_err();
        assert!(!err.is_panic());
        assert!(err.is_cancelled());
    }

    #[tokio::test]
    async fn test_resolve_task_handle_after_cancel() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
    
        queue.spawn(move |_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));

        let handle = queue.spawn(move |_| Box::pin(async {}));

        rx.await.unwrap();
        handle.cancel();
        let err = handle.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(!err.is_panic());
    }

    #[tokio::test]
    async fn test_resolve_task_handle_after_queue_aborted() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();

        let handle1 = queue.spawn(move |_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let handle2 = queue.spawn(move |_| Box::pin(async {}));

        rx.await.unwrap();
        drop(queue);

        let err1 = handle1.await.unwrap_err();
        assert!(err1.is_cancelled());
        assert!(!err1.is_panic());
        let err2 = handle2.await.unwrap_err();
        assert!(err2.is_cancelled());
        assert!(!err2.is_panic());
    }

    #[tokio::test]
    async fn test_resolve_task_handle_after_queue_canncelled() {
        let queue = TaskQueue::new(Vec::new());
        let (tx, rx) = oneshot::channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();

        let handle1 = queue.spawn(move |state| Box::pin(async move {
            tx.send(()).unwrap();
            while let Some(v) = rx2.recv().await {
                state.push(v);
            }
            state.clone()
        }));
        let handle2 = queue.spawn(move |_| Box::pin(async {
            pending::<()>().await;
        }));

        tx2.send(0).unwrap();
        rx.await.unwrap();
        
        queue.cancel();

        let err2 = handle2.await.unwrap_err();
        assert!(err2.is_cancelled());
        assert!(!err2.is_panic());

        tx2.send(1).unwrap();
        drop(tx2);

        assert_eq!(handle1.await.unwrap(), vec![0, 1]);
    }

    #[tokio::test]
    async fn test_cancel_join() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();

        let running = queue.spawn(move |_| Box::pin(async move {
            tx.send(()).unwrap();
            sleep(Duration::from_millis(500)).await;
            "complete"
        }));

        let pending = queue.spawn(move |_| Box::pin(async move { }));

        rx.await.unwrap();
        queue.cancel_and_join().await;
        assert_eq!(running.await.unwrap(), "complete");
        assert!(pending.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn test_task_panic_err_state() {
        let queue = TaskQueue::new(());

        let handle = queue.spawn(move |_| Box::pin(async move {
            panic!();
        }));
        let err = handle.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.kind().is_task_panic());
        assert!(!err.kind().is_previous_task_panic());
        assert!(!err.is_cancelled());

        let handle = queue.spawn(move |_| Box::pin(async move {}));
        let err = handle.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.kind().is_previous_task_panic());
        assert!(!err.kind().is_task_panic());
        assert!(!err.is_cancelled());
    }

    #[tokio::test]
    async fn test_task_handle_cancel_err_state() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
        let _ = queue.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let pending = queue.spawn(|_| Box::pin(async {

        }));
        rx.await.unwrap();

        pending.cancel();
        drop(queue);

        let err = pending.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(err.kind().is_task_cancelled());
        assert!(!err.kind().is_queue_aborted());
        assert!(!err.kind().is_queue_cancelled());
    }

    #[tokio::test]
    async fn test_task_canceller_cancel_err_state() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
        let _ = queue.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let pending = queue.spawn(|_| Box::pin(async {

        }));
        rx.await.unwrap();

        pending.canceller().cancel();
        drop(queue);

        let err = pending.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(err.kind().is_task_cancelled());
        assert!(!err.kind().is_queue_aborted());
        assert!(!err.kind().is_queue_cancelled());
    }

    #[tokio::test]
    async fn test_worker_abort_err_state() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
        let _ = queue.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let pending = queue.spawn(|_| Box::pin(async {

        }));
        rx.await.unwrap();

        drop(queue);
        let err = pending.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(!err.kind().is_task_cancelled());
        assert!(err.kind().is_queue_aborted());
        assert!(!err.kind().is_queue_cancelled());
    }

    #[tokio::test]
    async fn test_worker_cancel_err_state() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
        let _ = queue.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            pending::<()>().await;
        }));
        let pending = queue.spawn(|_| Box::pin(async {

        }));
        rx.await.unwrap();

        queue.cancel();
        let err = pending.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(!err.kind().is_task_cancelled());
        assert!(!err.kind().is_queue_aborted());
        assert!(err.kind().is_queue_cancelled());
    }

    #[tokio::test]
    async fn test_panic_err_state_after_worker_cancel() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
        let _ = queue.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            panic!()
        }));
        let pending = queue.spawn(|_| Box::pin(async {

        }));
        rx.await.unwrap();

        queue.cancel();
        let err = pending.await.unwrap_err();
        assert!(err.is_panic());
        assert!(!err.kind().is_task_panic());
        assert!(err.kind().is_previous_task_panic());
        assert!(!err.is_cancelled());
    }

    #[tokio::test]
    async fn test_execute() {
        let queue = TaskQueue::new(0);

        for _ in 0..10000 {
            queue.execute(|s| Box::pin(async {
                *s += 1;
            })).await;

            queue.execute_blocking(|s| {
                *s += 1;
            }).await;
        }
        
        assert_eq!(queue.join().await, 20000);
    }

    #[tokio::test]
    #[should_panic]
    async fn test_execute_panicked() {
        let queue = TaskQueue::new(());
        queue.execute(|_| Box::pin(async { panic!() })).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test_execute_blocking_panicked() {
        let queue = TaskQueue::new(());
        queue.execute_blocking(|_| { panic!() }).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test_execute_panicked_after_panic() {
        let queue = TaskQueue::new(());
        let h = queue.spawn(|_| Box::pin(async { panic!()}));
        assert!(h.await.unwrap_err().kind().is_task_panic());
        queue.execute(|_| Box::pin(async { })).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test_execute_blocking_panicked_after_panic() {
        let queue = TaskQueue::new(());
        let h = queue.spawn_blocking(|_| {});
        assert!(h.await.unwrap_err().kind().is_task_panic());
        queue.execute_blocking(|_| {}).await;
    }

    #[tokio::test]
    async fn test_executer_panicked() {
        let queue = TaskQueue::new(());
        assert!(!queue.has_panicked());
        let _ = queue.spawn_blocking(|_| { }).await; 
        assert!(!queue.has_panicked());
        let h = queue.spawn_blocking(|_| { panic!() }); 
        let r = h.await;
        assert!(queue.has_panicked());
        assert!(r.unwrap_err().kind().is_task_panic());

        let queue = TaskQueue::new(());
        let s = queue.spawner();
        let h = s.spawn_blocking(|_| { panic!() }); 
        let r = h.await;
        assert!(s.has_panicked().unwrap());
        assert!(queue.has_panicked());
        assert!(r.unwrap_err().kind().is_task_panic());
    }

    #[tokio::test]
    async fn test_spawner_on_outside_tokio_context() {
        let queue = TaskQueue::default();
        let spawner = queue.spawner();
        
        std::thread::spawn(move || {
            assert!(tokio::runtime::Handle::try_current().unwrap_err().is_missing_context());

            spawner.spawn(|s: &mut Vec<_>| Box::pin(async {
                s.push(0);
            }));
            spawner.spawn_blocking(|s: &mut Vec<_>| {
                s.push(1);
            });
        }).join().unwrap();

        assert_eq!(queue.join().await, vec![0, 1]);
    }

    #[tokio::test]
    async fn test_cancel_after_task_panic() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
        let h = queue.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            sleep(Duration::from_secs(1)).await;
            panic!()
        }));
        rx.await.unwrap();

        assert!(queue.cancel_and_try_join().await.is_err());
        assert!(h.await.unwrap_err().kind().is_task_panic());
    }

    #[tokio::test]
    async fn test_join_after_task_panic() {
        let queue = TaskQueue::new(());
        let (tx, rx) = oneshot::channel();
        let h = queue.spawn(|_| Box::pin(async {
            tx.send(()).unwrap();
            sleep(Duration::from_secs(1)).await;
            panic!()
        }));
        rx.await.unwrap();

        assert!(queue.try_join().await.is_err());
        assert!(h.await.unwrap_err().kind().is_task_panic());
    }
}

#[cfg(test)]
mod tests2 {
    use tokio::sync::oneshot;
    use super::*;


    #[tokio::test]
    async fn test() {
        let queue = TaskQueue::new(Vec::new());

        queue.spawn(move |state| Box::pin(async move {
            state.push(identity("1").await);
        }));

        let spawner = queue.spawner();
        tokio::spawn(async move {
            spawner.spawn_blocking(move |state| {
                state.push("2");
            });

            let task_handle = spawner.spawn(move |state| Box::pin(async move {
                state.push("3");
                "hello world"
            }));
            assert_eq!(task_handle.await.unwrap(), "hello world");
        });

        // Wait for all tasks to complete.
        // NOTE: This does not complete as long as `spawner` has not been dropped.
        let result = queue.join().await;
        assert_eq!(result, vec!["1", "2", "3"]);

        async fn identity<T>(v: T) -> T { v }
    }

    #[tokio::test]
    async fn test0() {
        let queue = TaskQueue::new(Vec::new());

        queue.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
            state.push(identity(0).await);
        }));

        queue.spawn_blocking(move |state| {
            state.push(1);
        });

        let task_result = queue.spawn(move |state| Box::pin(async move {
            state.push(identity(2).await);
            "hello"
        })).await.unwrap();
        assert_eq!(task_result, "hello");

        let task_result = queue.spawn_blocking(move |state| {
            state.push(3);
            "world"
        }).await.unwrap();
        assert_eq!(task_result, "world");

        let result = queue.join().await;
        assert_eq!(result, vec![0, 1, 2, 3]);

        async fn identity(v: u64) -> u64 {
            v
        }
    }

    #[tokio::test]
    async fn test1() {
        let queue = TaskQueue::new(Vec::new());
        let c = 1000;

        for i in 0..c {
            if i % 2 == 0 {
                queue.spawn(move |state| Box::pin(async move { state.push(i); }));
            }
            else {
                queue.spawn_blocking(move |state| { state.push(i); });
            }
        }

        assert_eq!(queue.join().await, (0..c).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test2() {
        let queue = TaskQueue::new(Vec::new());
        let c = 1000;

        for i in 0..c {
            if i % 2 == 0 {
                let r = queue.spawn(move |state| Box::pin(async move { 
                    state.push(i); 
                    i
                })).await;

                assert_eq!(r.unwrap(), i);
            }
            else {
                let r = queue.spawn_blocking(move |state| { 
                    state.push(i); 
                    i
                }).await;

                assert_eq!(r.unwrap(), i);
            }
        }

        assert_eq!(queue.join().await, (0..c).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test3() {
        let queue = TaskQueue::new(Vec::new());
        let c = 1000;

        for i in 0..c {
            if i % 2 == 0 {
                queue.spawn(move |state| Box::pin(async move { state.push(i); })).await.unwrap();
            }
            else {
                queue.spawn_blocking(move |state| { state.push(i); }).await.unwrap();
            }
        }

        assert_eq!(queue.join().await, (0..c).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test4() {
        let queue = TaskQueue::new(vec![0]);
        assert_eq!(queue.join().await, vec![0]);
    }

    #[tokio::test]
    async fn test5() {
        let queue = TaskQueue::new(vec![0]);
        assert_eq!(queue.try_join().await.unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn test6() {
        let queue = TaskQueue::new(vec![0]);
        queue.spawn(|_| Box::pin(async { panic!() }));
        let r = queue.try_join().await;
        assert!(r.as_ref().is_err());
        assert!(r.as_ref().is_err());
    }

    #[tokio::test]
    async fn test7() {
        {
            let queue = TaskQueue::new(());
        
            let (tx, rx) = oneshot::channel();
            let handle = queue.spawn(move |_| Box::pin(async {
                rx.await.unwrap();
            }));

            drop(queue);
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
        {
            let queue = TaskQueue::new(());
        
            let (tx, rx) = oneshot::channel();
            let handle = queue.spawn_blocking(move |_| {
                rx.blocking_recv().unwrap();
            });

            drop(queue);
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
    }

    #[tokio::test]
    async fn test8() {
        {
            let queue = TaskQueue::new(());
        
            let handle = queue.spawn(move |_| Box::pin(async {
                panic!()
            }));

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| !e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.is_panic()));
            assert!(r.as_ref().is_err_and(|e| e.kind().is_task_panic()));
        }
        {
            let queue = TaskQueue::new(());
        
            let handle = queue.spawn_blocking(move |_| {
                panic!("this is a panic message")
            });

            let r = handle.await;
            eprintln!("{}", r.as_ref().unwrap_err());
            assert!(r.as_ref().is_err_and(|e| !e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.is_panic()));
            assert!(r.as_ref().is_err_and(|e| e.kind().is_task_panic()));
        }
    }

    #[tokio::test]
    async fn test9() {
        {
            let queue = TaskQueue::new(());
        
            let (tx, rx) = oneshot::channel();
            let handle = queue.spawn(move |_| Box::pin(async {
                rx.await.unwrap();
                panic!()
            }));

            drop(queue);
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
        {
            let queue = TaskQueue::new(());
        
            let (tx, rx) = oneshot::channel();
            let handle = queue.spawn_blocking(move |_| {
                rx.blocking_recv().unwrap();
                panic!()
            });

            drop(queue);
            tx.send(()).unwrap();

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| !e.is_panic()));
        }
    }

    #[tokio::test]
    async fn test10() {
        {
            let queue = TaskQueue::new(());
        
            queue.spawn(move |_| Box::pin(async {
                panic!()
            }));
            let handle = queue.spawn(move |_| Box::pin(async {
                
            }));

            let r = handle.await;
            assert!(r.as_ref().is_err_and(|e| !e.is_cancelled()));
            assert!(r.as_ref().is_err_and(|e| e.is_panic()));
        }
        {
            let queue = TaskQueue::new(());
        
            queue.spawn_blocking(move |_| {
                panic!()
            });
            let handle = queue.spawn_blocking(move |_| {
                
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
        let se = TaskQueue::new("0".to_string());

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
        let se = TaskQueue::new(0);

        #[allow(unused_must_use)] {
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
        }

        let result = se.join().await;

        assert_eq!(result, 9)
    }

    #[tokio::test]
    async fn test3() {
        let se = Arc::new(TaskQueue::new(0));

        let mut set = tokio::task::JoinSet::new();
        let i = 10000;
        for _ in 0..i {
            let se1 = Arc::clone(&se);
            let se2 = Arc::clone(&se);
            set.spawn(async move {
                se1.spawn(|ctx| Box::pin(async{*ctx += 1;})).await.unwrap();
            });
            set.spawn(async move {
                se2.spawn_blocking(|ctx| {*ctx += 1;}).await.unwrap();
            });
        }

        set.join_all().await;

        let result = Arc::into_inner(se).unwrap().join().await;

        assert_eq!(result, i * 2)
    }

    #[tokio::test]
    async fn test4() {
        let se = TaskQueue::new(0);
        let result = se.join().await;
        assert_eq!(result, 0)
    }

    #[tokio::test]
    #[should_panic]
    async fn test6() {
        let se = TaskQueue::new(Vec::<&'static str>::new());

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        })).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test7() {
        let se = TaskQueue::new(Vec::<&'static str>::new());

        se.spawn_blocking(|_ctx| {
            panic!()
        }).await.unwrap();
    }

    #[tokio::test]
    async fn test8() {
        let se = TaskQueue::new(Vec::<&'static str>::new());

        // TaskHandle を待機しないと task で panic しても sumit_blocking は panic しない
        se.spawn_blocking(|_ctx| {
            panic!()
        });
    }

    #[tokio::test]
    async fn test9() {
        let se = TaskQueue::new(Vec::<&'static str>::new());

        // TaskHandle を待機しないと task で panic しても sumit は panic しない
        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));
    }

    #[tokio::test]
    #[should_panic]
    async fn test10() {
        let se = TaskQueue::new(Vec::<&'static str>::new());

        se.spawn_blocking(|_ctx| {
            panic!()
        }).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test11() {
        let se = TaskQueue::new(Vec::<&'static str>::new());

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        })).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test12() {
        let se = TaskQueue::new(Vec::<&'static str>::new());

        // task が panic　した場合、その後の task も panic　になる。

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));

        se.spawn_blocking(|_ctx| {}).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test13() {
        let se = TaskQueue::new(Vec::<&'static str>::new());

        se.spawn(|_ctx| Box::pin(async {
            panic!()
        }));

        se.join().await;
    }

    #[tokio::test]
    async fn test14() {
        let se = TaskQueue::new(Vec::<u64>::new());
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
        let se = TaskQueue::new(Vec::<u64>::new());
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
        let se = TaskQueue::new(());
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
        let queue = TaskQueue::new(Vec::<u64>::new());

        let handle = queue.spawn(|state| Box::pin(async move {
            state.push(1);
        }));

        drop(handle);

        let result = queue.join().await;

        assert_eq!(result, vec![1]);
    }

    #[tokio::test]
    async fn test18() {
        let queue = TaskQueue::new(Vec::<&'static str>::new());

        queue.spawn(|state| Box::pin(async move {
            state.push("async-1");
            tokio::task::yield_now().await;
            state.push("async-2");
        }));

        queue.spawn_blocking(|state| {
            state.push("blocking");
        });

        queue.spawn(|state| Box::pin(async move {
            state.push("async-3");
        }));

        assert_eq!(
            queue.join().await,
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
        let queue = TaskQueue::new(());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let handle = queue.spawn(move |_| Box::pin(async move {
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));

        started_rx.await.unwrap();

        drop(queue);

        handle.await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test20() {
        let queue = TaskQueue::new(Vec::<u64>::new());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        queue.spawn(move |state| Box::pin(async move {
            state.push(1);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));

        started_rx.await.unwrap();

        let queued_handle = queue.spawn(|state| Box::pin(async move {
            state.push(2);
        }));

        drop(queue);

        queued_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test21() {
        let queue = TaskQueue::new(Vec::<u64>::new());

        for i in 0..1000 {
            queue.spawn(move |state| Box::pin(async move {
                state.push(i);
            }));
        }

        let result = queue.join().await;

        assert_eq!(result, (0..1000).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test22() {
        let se = TaskQueue::new(0);
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