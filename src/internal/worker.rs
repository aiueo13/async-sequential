use super::*;
use std::{panic::{RefUnwindSafe, UnwindSafe}, sync::{Arc, Mutex as SyncMutex, OnceLock, atomic::{AtomicU8, Ordering}}};
use tokio::{sync::{Mutex, mpsc}, task::{JoinHandle, spawn, spawn_blocking}};


pub enum WorkerRuntime<'a> {
    Current,
    Handle(&'a tokio::runtime::Handle)
}

impl<'a> WorkerRuntime<'a> {

    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self {
            WorkerRuntime::Current => spawn(future),
            WorkerRuntime::Handle(handle) => handle.spawn(future),
        }
    }
}

pub fn spawn_worker<S: Send + 'static>(
    state: S,
    runtime: WorkerRuntime<'_>,
) -> WorkerHandle<S> {

    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task<S>>();
    let worker_status = Arc::new(WorkerStatus::new());

    let state = Arc::new(Mutex::new(Some(state)));

    let worker_handle = {
        let worker_status = Arc::clone(&worker_status);
        let mut state = Arc::clone(&state);
        runtime.spawn(async move {
            while let Some(task) = task_rx.recv().await {
                if matches!(worker_status.flag(), WorkerFlagSnapshot::CancelStarted) {
                    break;
                }
                let Some((task, task_panic_sender)) = task.take_if_not_cancelled() else {
                    continue;
                };

                let result = match task {
                    RawTask::Blocking(task) => {
                        let result = spawn_blocking(move || {
                            let result = {
                                if let Ok(mut state) = state.try_lock() {
                                    if let Some(state) = state.as_mut() {
                                        catch_unwind(|| task(state))
                                    }
                                    else {
                                        // join により state が取られている。
                                        // この場合 tokio runtime がシャットダウンか abort されている。
                                        return None
                                    }
                                }
                                else {
                                    // 即座に lock を取得できない場合、
                                    // join と競合したということであり
                                    // この場合 tokio runtime がシャットダウンか abort されている。
                                    return None
                                }
                            };
                            Some((state, result))
                        }).await;
                        match result {
                            Ok(Some((s, result))) => {
                                state = s;
                                result
                            },
                            Ok(None) | Err(_) => {
                                // tokio runtime がシャットダウンか abort された場合、ここに来る可能性がある
                                return Err(())
                            },
                        }
                    },
                    RawTask::Async(task) => {
                        if let Ok(mut state) = state.try_lock() {
                            if let Some(state) = state.as_mut() {
                                catch_unwind_async(|| task(state)).await
                            }
                            else {
                                // join により state が取られている。
                                // この場合 tokio runtime がシャットダウンか abort されている。
                                return Err(())
                            }
                        }
                        else {
                            // 即座に lock を取得できない場合、
                            // join と競合したということであり
                            // この場合 tokio runtime がシャットダウンか abort されている。
                            return Err(())
                        }
                    },
                };

                if let Err(panic) = result {
                    let panic_msg = panic.msg().map(|m| Arc::new(m.to_string()));
                    worker_status.set_task_panicked(panic_msg);
                    task_panic_sender.send(panic);
                    return Err(())
                }
            }
            Ok(())
        })
    };

    WorkerHandle::new(state, worker_handle, worker_status, task_tx)
}

pub struct WorkerHandle<S> {
    worker_handle: JoinHandle<Result<(), ()>>,
    worker_status: Arc<WorkerStatus>,
    state: Arc<Mutex<Option<S>>>,
    task_tx: mpsc::UnboundedSender<Task<S>>,
    shared_task_senders_ctx: Arc<SyncMutex<Option<WorkerTaskSenderCtx<S>>>>,
}

impl<S: UnwindSafe> UnwindSafe for WorkerHandle<S> {}
impl<S: RefUnwindSafe> RefUnwindSafe for WorkerHandle<S> {}

impl<S> WorkerHandle<S> {

    fn new(
        state: Arc<Mutex<Option<S>>>,
        worker_handle: JoinHandle<Result<(), ()>>,
        worker_status: Arc<WorkerStatus>,
        task_tx: mpsc::UnboundedSender<Task<S>>,
    ) -> Self {

        let shared_task_senders_ctx = Arc::new(SyncMutex::new(Some(WorkerTaskSenderCtx {
            task_tx: task_tx.clone(),
            worker_status: Arc::clone(&worker_status)
        })));

        Self { state, worker_handle, worker_status, task_tx, shared_task_senders_ctx }
    }

    pub fn abort(mut self) {
        // worker_status では Tokio runtime のシャットダウンを検知できないので
        // JoinHandle::is_finished で確認する。
        // 競合もあり得るが、これは許容する。
        if !self.worker_handle.is_finished() {
            if self.worker_status.try_set_abort_started() {
                self.worker_handle.abort();
            }
        }

        // 全ての task_tx を破棄する。
        self.close_senders();
        drop(self.shared_task_senders_ctx);
        drop(self.task_tx);
    }

    pub fn cancel(mut self) {
        // worker_status では Tokio runtime のシャットダウンを検知できないので
        // JoinHandle::is_finished で確認する。
        // 競合もあり得るが、これは許容する。
        if !self.worker_handle.is_finished() {
            self.worker_status.try_set_cancel_started();
        }

        // 全ての task_tx を破棄する。
        self.close_senders();
        drop(self.shared_task_senders_ctx);
        drop(self.task_tx);
    }
    
    pub async fn abort_and_join(mut self) -> WorkerJoinError<S> {
        // worker_status では Tokio runtime のシャットダウンを検知できないので
        // JoinHandle::is_finished で確認する。
        // 競合もあり得るが、これは許容する。
        if !self.worker_handle.is_finished() {
            if self.worker_status.try_set_abort_started() {
                self.worker_handle.abort();
            }
        }

        // 全ての task_tx を破棄する。
        self.close_senders();
        drop(self.shared_task_senders_ctx);
        drop(self.task_tx);

        let result = Self::join_worker(self.worker_handle, self.state, self.worker_status).await;
        match result {
            Ok(state) => WorkerJoinError::WorkerAborted { poisoned_state: state },
            Err(err) => err,
        }
    }

    pub async fn cancel_and_join(mut self) -> Result<S, WorkerJoinError<S>> {
        // worker_status では Tokio runtime のシャットダウンを検知できないので
        // JoinHandle::is_finished で確認する。
        // 競合もあり得るが、これは許容する。
        if !self.worker_handle.is_finished() {
            self.worker_status.try_set_cancel_started();
        }

        // 全ての task_tx を破棄する。
        self.close_senders();
        drop(self.shared_task_senders_ctx);
        drop(self.task_tx);

        Self::join_worker(self.worker_handle, self.state, self.worker_status).await
    }

    pub async fn join(self) -> Result<S, WorkerJoinError<S>> {
        // WorkerHandleが持っている全ての task_tx を破棄し、
        // 全ての WorkerTaskSender が破棄された時点で join が終わるようにする。
        drop(self.shared_task_senders_ctx);
        drop(self.task_tx);

        Self::join_worker(self.worker_handle, self.state, self.worker_status).await
    }

    async fn join_worker(
        worker_handle: JoinHandle<Result<(), ()>>,
        state: Arc<Mutex<Option<S>>>,
        worker_status: Arc<WorkerStatus>
    ) -> Result<S, WorkerJoinError<S>> {

        let worker_result = worker_handle.await;

        // worker task は終了したが、
        // 内部のブロッキングタスクがまだ実行中である場合はそれが終わるまでここで待機する。
        let state = state.lock().await.take().expect("state should be exist");

        match worker_result {
            Ok(Ok(())) => Ok(state),
            Ok(Err(())) => {
                let poisoned_state = state;
                match worker_status.flag() {
                    WorkerFlagSnapshot::AbortStarted => {
                        Err(WorkerJoinError::WorkerAborted { poisoned_state })
                    },
                    WorkerFlagSnapshot::TaskPanicked => {
                        let panic_msg = worker_status.task_panic_msg();
                        Err(WorkerJoinError::AnyTaskPanic { panic_msg, poisoned_state })
                    },
                    // キャンセルは正常終了させるのでここに来た場合は tokio runtime がシャットダウンされたということ。
                    WorkerFlagSnapshot::Other | WorkerFlagSnapshot::CancelStarted => {
                        Err(WorkerJoinError::RuntimeShutdown { poisoned_state }) 
                    },
                }
            }
            Err(_) => {
                let poisoned_state = state;
                Err(WorkerJoinError::RuntimeShutdown { poisoned_state }) 
            }
        }
    }

    /// 既存の sender を全て close する。
    /// 今後生成される sender には影響しない。
    pub fn close_senders(&mut self) {
        // 既存の全ての WorkerTaskSender が保持する ctx を削除する。
        let ctx = self.shared_task_senders_ctx.lock().unwrap_or_else(|e| e.into_inner()).take();
        self.shared_task_senders_ctx = Arc::new(SyncMutex::new(ctx));
    }

    pub fn has_panicked(&self) -> bool {
        matches!(self.worker_status.flag(), WorkerFlagSnapshot::TaskPanicked)
    }

    pub fn sender(&self) -> WorkerTaskSender<S> {
        WorkerTaskSender::new(Arc::clone(&self.shared_task_senders_ctx))
    }

    pub fn send(&self, task: Task<S>) -> Result<Arc<WorkerStatus>, WorkerSendError> {
        match self.task_tx.send(task) {
            Ok(_) => Ok(Arc::clone(&self.worker_status)),
            Err(_) => {
                match self.worker_status.flag() {
                    WorkerFlagSnapshot::TaskPanicked => {
                        let panic_msg = self.worker_status.task_panic_msg();
                        Err(WorkerSendError::PrevTaskPanic { panic_msg })
                    },
                    WorkerFlagSnapshot::Other => {
                        Err(WorkerSendError::RuntimeShutdown)
                    },
                    // abort や cancel は self を取るのでここに来ない。
                    WorkerFlagSnapshot::AbortStarted | WorkerFlagSnapshot::CancelStarted => {
                        unreachable!()
                    },
                }
            },
        }
    }
}

struct WorkerTaskSenderCtx<S> {
    task_tx: mpsc::UnboundedSender<Task<S>>,
    worker_status: Arc<WorkerStatus>,
}

pub struct WorkerTaskSender<S> {
    shared_ctx: Arc<SyncMutex<Option<WorkerTaskSenderCtx<S>>>>,
}

impl<S> WorkerTaskSender<S> {

    fn new(shared_ctx: Arc<SyncMutex<Option<WorkerTaskSenderCtx<S>>>>) -> Self {
        Self { shared_ctx }
    }

    pub fn is_unavailable(&self) -> bool {
        let locked_ctx = self.shared_ctx.lock().unwrap_or_else(|e| e.into_inner());
        locked_ctx.is_none()
    }

    pub fn has_panicked(&self) -> Option<bool> {
        let locked_ctx = self.shared_ctx.lock().unwrap_or_else(|e| e.into_inner());
        let Some(ctx) = locked_ctx.as_ref() else {
            return None;
        };

        match ctx.worker_status.flag() {
            WorkerFlagSnapshot::AbortStarted => None,
            WorkerFlagSnapshot::CancelStarted => None,
            WorkerFlagSnapshot::TaskPanicked => Some(true),
            WorkerFlagSnapshot::Other => Some(false),
        }
    }
}

impl<S: Send + 'static> WorkerTaskSender<S> {

    pub fn send(&self, task: Task<S>) -> Result<Arc<WorkerStatus>, WorkerSendError> {
        let (result, worker_status) = {
            let locked_ctx = self.shared_ctx.lock().unwrap_or_else(|e| e.into_inner());
            let Some(ctx) = locked_ctx.as_ref() else {
                return Err(WorkerSendError::TaskSenderUnavailable)
            };
            let worker_status = Arc::clone(&ctx.worker_status);
            let result = ctx.task_tx.send(task);
            (result, worker_status)
        };
        
        match result {
            Ok(_) => Ok(worker_status),
            Err(_) => {
                match worker_status.flag() {
                    WorkerFlagSnapshot::AbortStarted |
                    WorkerFlagSnapshot::CancelStarted => {
                        Err(WorkerSendError::TaskSenderUnavailable)
                    },
                    WorkerFlagSnapshot::TaskPanicked => {
                        let panic_msg = worker_status.task_panic_msg();
                        Err(WorkerSendError::PrevTaskPanic { panic_msg })
                    },
                    WorkerFlagSnapshot::Other => {
                        Err(WorkerSendError::RuntimeShutdown)
                    },
                }
            },
        }
    }
}

pub struct WorkerStatus {
    flags: AtomicU8,
    task_panic_msg: OnceLock<Option<Arc<String>>>,
}

impl WorkerStatus {

    fn new() -> Self {
        Self {
            flags: AtomicU8::new(Self::FLAG_OTHER),
            task_panic_msg: OnceLock::new(),
        }
    }
    
    /// タスクがパニックしてワーカーが終了し、かつそのパニックのメッセージがあればそれを取得する。
    /// flag() == WorkerFlagSnapshot::TaskPanicked を確認した後であれば、
    /// パニックのメッセージがあればそれを必ず取得できる。
    /// ただし、パニックのメッセージがない場合は None になることに注意
    pub fn task_panic_msg(&self) -> Option<Arc<String>> {
        self.task_panic_msg.get().and_then(|s| s.as_ref().map(Arc::clone))
    }

    pub fn flag(&self) -> WorkerFlagSnapshot {
        let flags = self.flags.load(Ordering::Acquire);
        match flags {
            Self::FLAG_OTHER => WorkerFlagSnapshot::Other,
            Self::FLAG_ABORT_STARTED => WorkerFlagSnapshot::AbortStarted,
            Self::FLAG_CANCEL_STARTED => WorkerFlagSnapshot::CancelStarted,
            Self::FLAG_TASK_PANICKED => WorkerFlagSnapshot::TaskPanicked,
            _ => unreachable!("WorkerStatus flags corrupted: {flags:#010b}"),
        }
    }

    /// このメソッドが呼ばれた時点で cancel/panic 済みなら設定しない。
    fn try_set_abort_started(&self) -> bool {
        self.flags
            .compare_exchange(
                Self::FLAG_OTHER,
                Self::FLAG_ABORT_STARTED,
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// このメソッドが呼ばれた時点で abort/panic 済みなら設定しない。
    fn try_set_cancel_started(&self) -> bool {
        self.flags
            .compare_exchange(
                Self::FLAG_OTHER,
                Self::FLAG_CANCEL_STARTED,
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// このメソッドが呼ばれた時点で abort/cancel 済みならそれを上書きする。
    /// すでに panic 済みであれば msg は設定しないが、
    /// このメソッドが複数回同時に呼ばれた場合、どれを使うかは問わない。
    fn set_task_panicked(&self, msg: Option<Arc<String>>) {
        // 先に panic msg を設定し、panicked であれば必ず panic msg があるようにする。
        let _ = self.task_panic_msg.set(msg);
        self.flags.store(Self::FLAG_TASK_PANICKED, Ordering::Release);
    }

    const FLAG_OTHER: u8 = 0b0000_0000;
    const FLAG_ABORT_STARTED: u8 = 0b0000_0001;
    const FLAG_CANCEL_STARTED: u8 = 0b0000_0010;
    const FLAG_TASK_PANICKED: u8 = 0b0000_0100;
}

pub enum WorkerFlagSnapshot {
    AbortStarted,
    CancelStarted,
    TaskPanicked,
    Other,
}

pub enum WorkerJoinError<S> {
    AnyTaskPanic {
        panic_msg: Option<Arc<String>>,
        poisoned_state: S
    },
    RuntimeShutdown {
        poisoned_state: S
    },
    WorkerAborted {
        poisoned_state: S
    }
}

pub enum WorkerSendError {
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>,
    },
    RuntimeShutdown,
    TaskSenderUnavailable,
}