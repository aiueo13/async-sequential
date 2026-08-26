use super::*;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use tokio::sync::oneshot;


pub fn build_async_task<S, T, R>(
    task: T,
) -> (Task<S>, TaskResultReceiver<R>, Arc<dyn TaskCanceller>)
where 
    S: Send + 'static,
    T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
    R: Send + 'static,
{
    let (task, result_rx) = build_async_raw_task(task);
    build_cancellable_task(task, result_rx)
}

pub fn build_blocking_task<S, T, R>(
    task: T,
) -> (Task<S>, TaskResultReceiver<R>, Arc<dyn TaskCanceller>)
where 
    S: Send + 'static,
    T: (FnOnce(&mut S) -> R) + Send + 'static,
    R: Send + 'static,
{
    let (task, result_rx) = build_blocking_raw_task(task);
    build_cancellable_task(task, result_rx)
}

pub fn build_uncancellable_async_task<S, T, R>(
    task: T,
) -> (Task<S>, TaskResultReceiver<R>)
where 
    S: Send + 'static,
    T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
    R: Send + 'static,
{
    let (task, result_rx) = build_async_raw_task(task);
    build_uncancellable_task(task, result_rx)
}

pub fn build_uncancellable_blocking_task<S, T, R>(
    task: T,
) -> (Task<S>, TaskResultReceiver<R>)
where 
    S: Send + 'static,
    T: (FnOnce(&mut S) -> R) + Send + 'static,
    R: Send + 'static,
{
    let (task, result_rx) = build_blocking_raw_task(task);
    build_uncancellable_task(task, result_rx)
}


fn build_async_raw_task<S, T, R>(
    task: T,
) -> (RawTask<S>, oneshot::Receiver<R>)
where 
    S: Send + 'static,
    T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
    R: Send + 'static
{
    let (result_tx, result_rx) = oneshot::channel();
    let task = RawTask::Async(Box::new(|s: &mut S| Box::pin(async {
        let _ = result_tx.send(task(s).await);
    })));
    (task, result_rx)
}

fn build_blocking_raw_task<S, T, R>(
    task: T,
) -> (RawTask<S>, oneshot::Receiver<R>)
where 
    S: Send + 'static,
    T: (FnOnce(&mut S) -> R) + Send + 'static,
    R: Send + 'static
{
    let (result_tx, result_rx) = oneshot::channel();
    let task = RawTask::Blocking(Box::new(move |s: &mut S| {
        let _ = result_tx.send(task(s));
    }));
    (task, result_rx)
}

fn build_cancellable_task<S, R>(
    task: RawTask<S>,
    result_rx: oneshot::Receiver<R>,
) -> (Task<S>, TaskResultReceiver<R>, Arc<dyn TaskCanceller>)
where 
    S: Send + 'static,
{
    let (panic_tx, panic_rx) = oneshot::channel();
    let panic_sender = TaskPanicSender::new(panic_tx);
    let task = Arc::new(OnceTake::new((task, panic_sender)));
    let canceller = Arc::new(TaskCancellerImpl::new(Arc::downgrade(&task)));
    let result = TaskResultReceiver::new(result_rx, panic_rx);
    (Task::new_cancellable(task), result, canceller)
}

fn build_uncancellable_task<S, R>(
    task: RawTask<S>,
    result_rx: oneshot::Receiver<R>,
) -> (Task<S>, TaskResultReceiver<R>)
where 
    S: Send + 'static,
{
    let (panic_tx, panic_rx) = oneshot::channel();
    let panic_sender = TaskPanicSender::new(panic_tx);
    let task = (task, panic_sender);
    let result = TaskResultReceiver::new(result_rx, panic_rx);
    (Task::new_uncancellable(task), result)
}


pub struct Task<S> {
    repr: TaskRepr<S>,
}

enum TaskRepr<S> {
    Uncancellable((RawTask<S>, TaskPanicSender)),
    Cancellable(Arc<OnceTake<(RawTask<S>, TaskPanicSender)>>)
}

pub enum RawTask<S> {
    Blocking(Box<dyn (FnOnce(&mut S) -> ()) + Send>),
    Async(Box<dyn (for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>) + Send>),
}

impl<S> Task<S> {

    fn new_uncancellable(task: (RawTask<S>, TaskPanicSender)) -> Self {
        Self { repr: TaskRepr::Uncancellable(task)}
    }

    fn new_cancellable(task: Arc<OnceTake<(RawTask<S>, TaskPanicSender)>>) -> Self {
        Self { repr: TaskRepr::Cancellable(task)}
    }

    pub fn take(self) -> Option<(RawTask<S>, TaskPanicSender)> {
        match self.repr {
            TaskRepr::Uncancellable(task) => Some(task),
            TaskRepr::Cancellable(task) => task.take(),
        }
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
    result_rx_ready: bool,
    panic_rx_ready: bool
}

impl<R> TaskResultReceiver<R> {

    fn new(
        result_rx: oneshot::Receiver<R>,
        panic_rx: oneshot::Receiver<PanicPayload>,
    ) -> Self {

        Self {
            result_rx, 
            panic_rx, 
            result_rx_ready: false,
            panic_rx_ready: false,
        }
    }
}

impl<R> Future for TaskResultReceiver<R> {
    type Output = Result<R, Option<PanicPayload>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        if !self.panic_rx_ready {
            if let Poll::Ready(poll) = Pin::new(&mut self.panic_rx).poll(cx) {
                self.panic_rx_ready = true;
                if let Ok(panic) = poll {
                    return Poll::Ready(Err(Some(panic)));
                }
                else {
                    // panic tx が送信されずに drop された場合
                }
            }
        }
        if !self.result_rx_ready {
            if let Poll::Ready(poll) = Pin::new(&mut self.result_rx).poll(cx) {
                self.result_rx_ready = true;
                if let Ok(result) = poll {
                    return Poll::Ready(Ok(result));
                }
                else {
                    // result tx が送信されずに drop された場合
                }
            }
        }

        if self.panic_rx_ready && self.result_rx_ready {
            Poll::Ready(Err(None))
        }
        else {
            Poll::Pending
        }
    }
}

pub trait TaskCanceller: Sync + Send + RefUnwindSafe + UnwindSafe + 'static {

    /// すでにキャンセルされているかを返す。
    fn is_cancelled(&self) -> bool;

    /// タスクが実行待機中の場合はタスクをキャンセルし、 true を返す。
    /// タスクが実行中か実行済みの場合は何も行わず false を返す。
    fn cancel(&self) -> bool;
}

struct TaskCancellerImpl<S> {
    task: Weak<OnceTake<S>>,
    is_cancelled: AtomicBool
}

impl<S> TaskCancellerImpl<S> {

    fn new(task: Weak<OnceTake<S>>) -> Self {
        Self {
            task,
            is_cancelled: AtomicBool::new(false)
        }
    }
}

impl<S: Send + 'static> TaskCanceller for TaskCancellerImpl<S> {

    fn cancel(&self) -> bool {
        let did_cancelled = self.task.upgrade().is_some_and(|task| task.take().is_some());
        if did_cancelled {
            self.is_cancelled.store(true, Ordering::Release);
        }
        did_cancelled
    }

    fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::Acquire)
    }
}