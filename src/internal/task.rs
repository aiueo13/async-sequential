use crate::*;
use std::pin::Pin;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use tokio::sync::oneshot;


pub fn build_async_task<S, T, R>(
    task: T,
) -> (Task<S>, TaskResultReceiver<R>, TaskController)
where 
    S: Send + 'static,
    T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    let task = Arc::new(OnceTake::new(TaskRepr::Async(Box::new(|s: &mut S| Box::pin(async {
        let _ = tx.send(task(s).await);
    })))));
    let controller = TaskController::new(Arc::downgrade(&task));

    (Task { task }, TaskResultReceiver { rx }, controller)
}

pub fn build_blocking_task<S, T, R>(
    task: T,
) -> (Task<S>, TaskResultReceiver<R>, TaskController)
where 
    S: Send + 'static,
    T: (FnOnce(&mut S) -> R) + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    let task = Arc::new(OnceTake::new(TaskRepr::Blocking(Box::new(move |s: &mut S| {
        let _ = tx.send(task(s));
    }))));
    let controller = TaskController::new(Arc::downgrade(&task));

    (Task { task }, TaskResultReceiver { rx }, controller)
}


pub struct Task<S> {
    task: Arc<OnceTake<TaskRepr<S>>>,
}

pub enum TaskRepr<S> {
    Blocking(Box<dyn (FnOnce(&mut S) -> ()) + Send>),
    Async(Box<dyn (for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>) + Send>),
}

impl<S> Task<S> {

    pub fn take(self) -> Option<TaskRepr<S>> {
        self.task.take()
    }
}

pub struct TaskResultReceiver<R> {
    rx: oneshot::Receiver<R>
}

impl<R> Future for TaskResultReceiver<R> {
    type Output = Option<R>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(t) => Poll::Ready(t.ok()),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
pub struct TaskController {
    repr: Arc<dyn (Fn(TaskControllerReprRequest) -> bool) + Sync + Send + 'static>
}

enum TaskControllerReprRequest {
    CancelIfQueueing,
    IsCancelled,
}

impl TaskController {

    fn new<S: Send + 'static>(task: Weak<OnceTake<TaskRepr<S>>>) -> Self {
        let is_cancelled = AtomicBool::new(false);
        let repr = Arc::new(move |request: TaskControllerReprRequest| {
            match request {
                TaskControllerReprRequest::CancelIfQueueing => {
                    let did_cancelled = task.upgrade().is_some_and(|task| task.take().is_some());
                    if did_cancelled {
                        is_cancelled.store(true, Ordering::Release);
                    }
                    did_cancelled
                },
                TaskControllerReprRequest::IsCancelled => {
                    is_cancelled.load(Ordering::Acquire)
                },
            }
        });

        Self { repr }
    }
}

impl TaskController {

    /// すでにキャンセルされているかを返す。
    pub fn is_cancelled(&self) -> bool {
        (self.repr)(TaskControllerReprRequest::IsCancelled)
    }

    /// タスクが実行待機中の場合はタスクをキャンセルし、 true を返す。
    /// タスクが実行中か実行済みの場合は何も行わず false を返す。
    pub fn cancel(&self) -> bool {
        (self.repr)(TaskControllerReprRequest::CancelIfQueueing)
    }
}