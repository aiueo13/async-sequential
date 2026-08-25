use super::*;
use std::{borrow::Cow, panic, sync::{Arc, Mutex as SyncMutex, OnceLock, atomic::{AtomicU8, Ordering}}};
use tokio::{sync::mpsc, task::{AbortHandle, JoinHandle, JoinError, spawn, spawn_blocking}};


pub fn spawn_worker<S: Send + 'static>(mut state: S) -> WorkerHandle<S> {
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task<S>>();
    let worker_state = Arc::new(WorkerState::new());
    let (panic_sender_tx, panic_sender_rx) = slot();

    let (worker_handle, worker_join_handle) = {
        let worker_state = Arc::clone(&worker_state);
        let worker_join_handle = spawn(async move {
            while let Some(task) = task_rx.recv().await {
                if worker_state.flags().is_cancelled() {
                    break;
                }

                let Some((task, panic_sender)) = task.take() else {
                    continue;
                };

                panic_sender_tx.set(panic_sender);

                match task {
                    RawTask::Blocking(task) => {
                        state = spawn_blocking(move || {
                            task(&mut state);
                            state
                        }).await.unwrap_or_else(|e| panic::resume_unwind(e.into_panic()));
                    },
                    RawTask::Async(task) => task(&mut state).await,
                }
            }
            state
        });
        (worker_join_handle.abort_handle(), worker_join_handle)
    };

    let join_handle = {
        let worker_state = Arc::clone(&worker_state);
        spawn(async move {
            match worker_join_handle.await {
                Ok(state) => Ok(state),
                Err(e) => {
                    let mut task_panic_msg = None;

                    let worker_flags = worker_state.flags();
                    let err = if worker_flags.is_cancelled() {
                        // 現在の実装ではここに来ることはない。
                        // cancel は worker を panic　させないためである。
                        WorkerJoinError::with_msg("worker canncelled")
                    }
                    else if worker_flags.is_aborted() {
                        // 現在の実装ではここに来ることはあるがこのエラーは使われない。
                        // abort は join を提供していないためである。
                        WorkerJoinError::with_msg("worker aborted")
                    }
                    else {
                        if let Some(panic) = e.try_into_panic().ok() {
                            let panic = PanicPayload::new(panic);
                            task_panic_msg = panic.msg().map(|s| s.to_string());
                            if let Some(panic_sender) = panic_sender_rx.take() { 
                                panic_sender.send(panic);
                            }
                        }

                        match &task_panic_msg {
                            Some(task_panic_msg) => WorkerJoinError::with_msg(task_panic_msg.clone()),
                            None => WorkerJoinError::with_no_msg(),
                        }
                    };

                    let _ = worker_state.set_task_panic_msg(task_panic_msg.map(Arc::new));
                    Err(err)
                },
            }
        })
    };

    WorkerHandle::new(worker_handle, join_handle, worker_state, task_tx)
}

pub struct WorkerHandle<S> {
    worker_handle: AbortHandle,
    join_handle: JoinHandle<Result<S, WorkerJoinError>>,
    worker_state: Arc<WorkerState>,
    task_tx: mpsc::UnboundedSender<Task<S>>,
    shared_task_senders_ctx: Arc<SyncMutex<Option<WorkerTaskSenderCtx<S>>>>,
}

impl<S> WorkerHandle<S> {

    fn new(
        worker_handle: AbortHandle,
        join_handle: JoinHandle<Result<S, WorkerJoinError>>,
        worker_state: Arc<WorkerState>,
        task_tx: mpsc::UnboundedSender<Task<S>>,
    ) -> Self {

        let shared_task_senders_ctx = Arc::new(SyncMutex::new(Some(WorkerTaskSenderCtx {
            task_tx: task_tx.clone(),
            worker_handle: worker_handle.clone()
        })));

        Self { worker_handle, join_handle, worker_state, task_tx, shared_task_senders_ctx }
    }

    pub fn abort(mut self) {
        if !self.worker_handle.is_finished() {
            self.worker_state.set_aborted();
            self.worker_handle.abort();
        }

        // 全ての task_tx を破棄する。
        self.close_task_senders();
        drop(self.shared_task_senders_ctx);
        drop(self.task_tx);
    }

    pub fn cancel(mut self) {
        if !self.worker_handle.is_finished() {
            self.worker_state.set_cancelled();
        }

        // 全ての task_tx を破棄する。
        self.close_task_senders();
        drop(self.shared_task_senders_ctx);
        drop(self.task_tx);
    }
    
    pub async fn cancel_and_join(mut self) -> Result<S, WorkerJoinError> {
        if !self.worker_handle.is_finished() {
            self.worker_state.set_cancelled();
        }

        // 全ての task_tx を破棄する。
        self.close_task_senders();
        drop(self.shared_task_senders_ctx);
        drop(self.task_tx);

        match self.join_handle.await {
            Ok(Ok(state)) => Ok(state),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e.into())
        }
    }

    pub async fn join(self) -> Result<S, WorkerJoinError> {
        // WorkerHandleが持っている全ての task_tx を破棄し、
        // 全ての WorkerTaskSender が破棄された時点で join が終わるようにする。
        drop(self.shared_task_senders_ctx);
        drop(self.task_tx);

        match self.join_handle.await {
            Ok(Ok(state)) => Ok(state),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e.into())
        }
    }

    pub fn close_task_senders(&mut self) {
        // 既存の全ての WorkerTaskSender が保持する ctx を削除する。
        let ctx = self.shared_task_senders_ctx.lock().unwrap_or_else(|e| e.into_inner()).take();
        self.shared_task_senders_ctx = Arc::new(SyncMutex::new(ctx));
    }

    pub fn has_panicked(&self) -> bool {
        if self.worker_handle.is_finished() {
            let f = self.worker_state.flags();
            !f.is_aborted() && !f.is_cancelled()
        }
        else {
            false
        }
    }

    pub fn sender(&self) -> WorkerTaskSender<S> {
        WorkerTaskSender {
            shared_ctx: Arc::clone(&self.shared_task_senders_ctx),
            worker_state: Arc::clone(&self.worker_state)
        }
    }

    pub fn send(&self, task: Task<S>) -> Result<Arc<WorkerState>, WorkerSendError> {
        match self.task_tx.send(task) {
            Ok(_) => Ok(Arc::clone(&self.worker_state)),
            Err(_) => {
                // abort と cancel は self を取るので、
                // send　の失敗の原因は過去のタスクのパニックに絞られる。
                let panic_msg = self.worker_state.task_panic_msg();
                Err(WorkerSendError::PrevTaskPanic { panic_msg })
            },
        }
    }
}

struct WorkerTaskSenderCtx<S> {
    task_tx: mpsc::UnboundedSender<Task<S>>,
    worker_handle: AbortHandle,
}

pub struct WorkerTaskSender<S> {
    shared_ctx: Arc<SyncMutex<Option<WorkerTaskSenderCtx<S>>>>,
    worker_state: Arc<WorkerState>,
}

impl<S> WorkerTaskSender<S> {

    pub fn is_unavailable(&self) -> bool {
        let locked_ctx = self.shared_ctx.lock().unwrap_or_else(|e| e.into_inner());
        locked_ctx.is_none()
    }

    pub fn has_panicked(&self) -> Option<bool> {
        let locked_ctx = self.shared_ctx.lock().unwrap_or_else(|e| e.into_inner());
        let Some(locked_ctx) = locked_ctx.as_ref() else {
            return None;
        };

        if locked_ctx.worker_handle.is_finished() {
            let f = self.worker_state.flags();
            let is_prev_task_panic = !f.is_aborted() && !f.is_cancelled();
            Some(is_prev_task_panic)
        }
        else {
            Some(false)
        }
    }
}

impl<S: Send + 'static> WorkerTaskSender<S> {

    pub fn send(&self, task: Task<S>) -> Result<Arc<WorkerState>, WorkerTaskSenderSendError> {
        let result = {
            let locked_ctx = self.shared_ctx.lock().unwrap_or_else(|e| e.into_inner());
            let Some(ref locked_ctx) = *locked_ctx else {
                return Err(WorkerTaskSenderSendError::Unavailable)
            };
            locked_ctx.task_tx.send(task)
        };
        
        match result {
            Ok(_) => Ok(Arc::clone(&self.worker_state)),
            Err(_) => {
                let worker_flags = self.worker_state.flags();
                if worker_flags.is_aborted() || worker_flags.is_cancelled() {
                    Err(WorkerTaskSenderSendError::Unavailable)
                }
                else {
                    let panic_msg = self.worker_state.task_panic_msg();
                    Err(WorkerTaskSenderSendError::PrevTaskPanic { panic_msg })
                }
            },
        }
    }
}

pub struct WorkerState {
    flags: AtomicU8,
    task_panic_msg: OnceLock<Option<Arc<String>>>,
}

impl WorkerState {

    pub fn flags(&self) -> WorkerFlagsSnapshot {
        WorkerFlagsSnapshot { flags: self.get_flags() }
    }
    
    /// タスクがパニックしてワーカーが終了し、かつそのパニックのメッセージがあればそれを取得する。
    /// これが None でもタスクがパニックしてワーカーが終了していることがあることに注意。
    pub fn task_panic_msg(&self) -> Option<Arc<String>> {
        self.task_panic_msg.get().and_then(|s| s.as_ref().map(Arc::clone))
    }
}

impl WorkerState {

    fn new() -> Self {
        Self {
            flags: AtomicU8::new(0),
            task_panic_msg: OnceLock::new()
        }
    }

    fn set_aborted(&self) {
        self.set_flag(Self::FLAG_ABORTED);
    }

    fn set_cancelled(&self) {
        self.set_flag(Self::FLAG_CANCELLED);
    }

    /// 既にセットされている場合はセットせず与えられた値をそのまま返す
    fn set_task_panic_msg(
        &self,
        msg: Option<Arc<String>>
    ) -> Result<(), Option<Arc<String>>> {

        self.task_panic_msg.set(msg)
    }


    const FLAG_ABORTED: u8 = 0b0000_0001;
    const FLAG_CANCELLED: u8 = 0b0000_0010;

    fn set_flag(&self, flag: u8) {
        self.flags.fetch_or(flag, Ordering::Release);
    }

    fn get_flags(&self) -> u8 {
        self.flags.load(Ordering::Acquire)
    }
}

pub struct WorkerFlagsSnapshot {
    flags: u8
}

impl WorkerFlagsSnapshot {
    
    pub fn is_aborted(&self) -> bool {
        self.flags & WorkerState::FLAG_ABORTED != 0
    }

    pub fn is_cancelled(&self) -> bool {
        self.flags & WorkerState::FLAG_CANCELLED != 0
    }
}

pub struct WorkerJoinError {
    panic_msg: Option<Cow<'static, str>>
}

impl WorkerJoinError {

    pub(super) fn with_msg(panic_msg: impl Into<Cow<'static, str>>) -> Self {
        Self { panic_msg: Some(panic_msg.into()) }
    }

    pub(super) fn with_no_msg() -> Self {
        Self { panic_msg: None }
    }

    pub fn into_panic_msg(self) -> Option<Cow<'static, str>> {
        self.panic_msg
    }

    pub fn panic_msg(&self) -> Option<&str> {
        self.panic_msg.as_deref()
    }
}

impl From<JoinError> for WorkerJoinError {

    fn from(value: JoinError) -> WorkerJoinError {
        let mut panic_msg = None;
        if let Some(panic) = value.try_into_panic().ok() {
            let panic = PanicPayload::new(panic);
            panic_msg = panic.msg().map(|s| s.to_string());
        }

        match panic_msg {
            Some(panic_msg) => WorkerJoinError::with_msg(panic_msg),
            None => WorkerJoinError::with_no_msg(),
        }
    }
}

pub enum WorkerSendError {
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>,
    },
}

pub enum WorkerTaskSenderSendError {
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>,
    },
    Unavailable,
}
