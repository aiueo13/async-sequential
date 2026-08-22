use crate::*;
use std::panic::{RefUnwindSafe, UnwindSafe};
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
    let (result_tx, result_rx) = oneshot::channel();
    let task = TaskRepr::Async(Box::new(|s: &mut S| Box::pin(async {
        let _ = result_tx.send(task(s).await);
    })));
    build_task(task, result_rx)
}

pub fn build_blocking_task<S, T, R>(
    task: T,
) -> (Task<S>, TaskResultReceiver<R>, TaskController)
where 
    S: Send + 'static,
    T: (FnOnce(&mut S) -> R) + Send + 'static,
    R: Send + 'static,
{
    let (result_tx, result_rx) = oneshot::channel();
    let task = TaskRepr::Blocking(Box::new(move |s: &mut S| {
        let _ = result_tx.send(task(s));
    }));
    build_task(task, result_rx)
}

fn build_task<S, R>(
    task: TaskRepr<S>,
    result_rx: oneshot::Receiver<R>,
) -> (Task<S>, TaskResultReceiver<R>, TaskController)
where 
    S: Send + 'static,
{
    let (panic_tx, panic_rx) = oneshot::channel();
    let panic_sender = TaskPanicSender::new(panic_tx);
    let task = Arc::new(OnceTake::new((task, panic_sender)));
    let controller = TaskController::new(Arc::downgrade(&task));
    (Task { task }, TaskResultReceiver { result_rx, panic_rx }, controller)
}


pub struct Task<S> {
    task: Arc<OnceTake<(TaskRepr<S>, TaskPanicSender)>>,
}

pub enum TaskRepr<S> {
    Blocking(Box<dyn (FnOnce(&mut S) -> ()) + Send>),
    Async(Box<dyn (for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>) + Send>),
}

impl<S> Task<S> {

    pub fn take(self) -> Option<(TaskRepr<S>, TaskPanicSender)> {
        self.task.take()
    }
}

pub struct TaskPanicSender {
    tx: oneshot::Sender<PanicPayload>
}

impl TaskPanicSender {

    fn new(tx: oneshot::Sender<PanicPayload>) -> Self {
        Self { tx }
    }

    pub fn send(self, panic: PanicPayload) {
        // 受信側が閉じていてもいい
        let _ = self.tx.send(panic);
    }
}

pub struct TaskResultReceiver<R> {
    result_rx: oneshot::Receiver<R>,
    panic_rx: oneshot::Receiver<PanicPayload>,
}

impl<R> Future for TaskResultReceiver<R> {
    type Output = Result<R, Option<PanicPayload>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        if let Poll::Ready(Ok(panic)) = Pin::new(&mut self.panic_rx).poll(cx) {
            return Poll::Ready(Err(Some(panic)));
        }

        match Pin::new(&mut self.result_rx).poll(cx) {
            Poll::Ready(Ok(payload)) => Poll::Ready(Ok(payload)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(None)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
pub struct TaskController {
    repr: Arc<dyn (Fn(TaskControllerReprRequest) -> bool) + Sync + Send + RefUnwindSafe + UnwindSafe + 'static>
}

enum TaskControllerReprRequest {
    Cancel,
    IsCancelled,
}

impl TaskController {

    fn new<S: Send + 'static>(task: Weak<OnceTake<S>>) -> Self {
        let is_cancelled = AtomicBool::new(false);
        let repr = Arc::new(move |request: TaskControllerReprRequest| {
            match request {
                TaskControllerReprRequest::Cancel => {
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
        (self.repr)(TaskControllerReprRequest::Cancel)
    }
}