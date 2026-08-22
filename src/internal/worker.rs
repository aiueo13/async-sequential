use crate::*;
use std::sync::{Arc, atomic::{AtomicU8, Ordering}};
use tokio::{spawn, sync::mpsc, task::{JoinHandle as SpawnJoinHandle, spawn_blocking}};


pub fn spawn_worker<S: Send + 'static>(mut state: S) -> WorkerHandle<S> {
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task<S>>();
    let worker_flags = Arc::new(WorkerFlags::new());
    let worker_handle = {
        let worker_flags = Arc::clone(&worker_flags);
        spawn(async move {
            while let Some(task) = task_rx.recv().await {
                if worker_flags.snapshot().is_cancelled() {
                    break;
                }

                match task.take() {
                    Some(TaskRepr::Blocking(task)) => {
                        state = spawn_blocking(move || {
                            task(&mut state);
                            state
                        }).await.expect("task panicked");
                    },
                    Some(TaskRepr::Async(task)) => task(&mut state).await,
                    None => {},
                }
            }
            state
        })
    };

    WorkerHandle { worker_handle, worker_flags, task_tx }
}

pub struct WorkerHandle<S> {
    worker_handle: SpawnJoinHandle<S>,
    worker_flags: Arc<WorkerFlags>,
    task_tx: mpsc::UnboundedSender<Task<S>>,
}

impl<S> WorkerHandle<S> {

    pub fn flgas(&self) -> Arc<WorkerFlags> {
        Arc::clone(&self.worker_flags)
    }

    pub fn send(&self, task: Task<S>) -> Result<(), ()> {
        self.task_tx.send(task).map_err(|_| ())
    }

    pub fn abort(self) {
        if !self.worker_handle.is_finished() {
            self.worker_flags.set_aborted();
            self.worker_handle.abort();
        }
    }

    pub fn cancel(self) {
        if !self.worker_handle.is_finished() {
            self.worker_flags.set_cancelled();
            drop(self.task_tx);
        }
    }
    
    pub async fn cancel_and_join(self) -> Result<S, TaskError> {
        if !self.worker_handle.is_finished() {
            self.worker_flags.set_cancelled();
            drop(self.task_tx);
        }

        match self.worker_handle.await {
            Ok(state) => Ok(state),
            Err(_) => {
                let worker_flags = self.worker_flags.snapshot();
                if worker_flags.is_cancelled() {
                    Err(TaskError::cancelled())
                }
                else if worker_flags.is_aborted() {
                    Err(TaskError::aborted())
                }
                else {
                    Err(TaskError::panicked())
                }
            }
        }
    }

    pub async fn join(self) -> Result<S, TaskError> {
        drop(self.task_tx);
        match self.worker_handle.await {
            Ok(state) => Ok(state),
            Err(_) => {
                let worker_flags = self.worker_flags.snapshot();
                if worker_flags.is_cancelled() {
                    Err(TaskError::cancelled())
                }
                else if worker_flags.is_aborted() {
                    Err(TaskError::aborted())
                }
                else {
                    Err(TaskError::panicked())
                }
            }
        }
    }
}

pub struct WorkerFlags {
    flags: AtomicU8,
}

impl WorkerFlags {

    pub fn new() -> Self {
        Self {
            flags: AtomicU8::new(0),
        }
    }

    pub fn snapshot(&self) -> WorkerStateFlagsSnapshot {
        WorkerStateFlagsSnapshot { flags: self.get_flags() }
    }

    const FLAG_ABORTED: u8 = 0b0000_0001;
    const FLAG_CANCELLED: u8 = 0b0000_0010;
    
    fn set_aborted(&self) {
        self.set_flag(Self::FLAG_ABORTED);
    }

    fn set_cancelled(&self) {
        self.set_flag(Self::FLAG_CANCELLED);
    }

    fn set_flag(&self, flag: u8) {
        self.flags.fetch_or(flag, Ordering::Release);
    }

    fn get_flags(&self) -> u8 {
        self.flags.load(Ordering::Acquire)
    }
}

pub struct WorkerStateFlagsSnapshot {
    flags: u8
}

impl WorkerStateFlagsSnapshot {
    
    pub fn is_cancelled(&self) -> bool {
        self.flags & WorkerFlags::FLAG_CANCELLED != 0
    }

    pub fn is_aborted(&self) -> bool {
        self.flags & WorkerFlags::FLAG_ABORTED != 0
    }
}