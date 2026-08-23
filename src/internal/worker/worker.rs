use crate::*;
use super::*;
use std::{sync::{Arc, Mutex as SyncMutex}};


pub struct Worker<S> {
    repr: SyncMutex<Option<Repr<S>>>,
}

enum Repr<S> {
    Unstarted {
        state: S,
    },
    Started {
        worker_handle: WorkerHandle<S>,
    },
}

impl<S> Worker<S> {

    pub const fn new(state: S) -> Self {
        Self {
            repr: SyncMutex::new(Some(Repr::Unstarted { state })),
        }
    }
    
    pub fn cancel(self) {
        let worker = self.repr.lock().unwrap().take();
        match worker {
            Some(Repr::Unstarted { .. }) => {},
            Some(Repr::Started { worker_handle }) => worker_handle.cancel(),
            None => unreachable!("illegal closed executor"),
        }
    }

    pub async fn cancel_and_join(self) -> Result<S, WorkerJoinError> {
        let worker = self.repr.lock().unwrap().take();
        match worker {
            Some(Repr::Unstarted { state }) => Ok(state),
            Some(Repr::Started { worker_handle }) => worker_handle.cancel_and_join().await,
            None => unreachable!("illegal closed executor"),
        }
    }

    pub async fn join(self) -> Result<S, WorkerJoinError> {
        let worker = self.repr.lock().unwrap().take();
        match worker {
            Some(Repr::Unstarted { state }) => Ok(state),
            Some(Repr::Started { worker_handle }) => worker_handle.join().await,
            None => unreachable!("illegal closed executor"),
        }
    }
}

impl<S: Send + 'static> Worker<S> {

    pub fn send(&self, task: Task<S>) -> Result<Arc<WorkerState>, WorkerSendError> {
        let mut locked_worker = self.repr.lock().unwrap();

        if let Some(Repr::Started { ref worker_handle, .. }) = *locked_worker {
            return worker_handle.send(task);
        }

        let Some(Repr::Unstarted { state }) = locked_worker.take() else {
            unreachable!("illegal closed executor")
        };

        let worker_handle = spawn_worker(state);
        let worker_state = match worker_handle.send(task) {
            Ok(worker_state) => worker_state,
            Err(WorkerSendError::PrevTaskPanic { .. }) => unreachable!(),
        };
        *locked_worker = Some(Repr::Started { worker_handle });
        Ok(worker_state)
    }

    pub fn weak_sender(&self) -> WeakWorkerTaskSender<S> {
        let mut locked_worker = self.repr.lock().unwrap();

        if let Some(Repr::Started { ref worker_handle, .. }) = *locked_worker {
            return worker_handle.weak_sender()
        }

        let Some(Repr::Unstarted { state }) = locked_worker.take() else {
            unreachable!("illegal closed executor")
        };

        let worker_handle = spawn_worker(state);
        let sender = worker_handle.weak_sender();
        *locked_worker = Some(Repr::Started { worker_handle });
        sender
    }
}

impl<S> Drop for Worker<S> {

    fn drop(&mut self) {
        match self.repr.lock().ok().and_then(|mut e| e.take()) {
            Some(Repr::Unstarted { .. }) => {},
            Some(Repr::Started { worker_handle, .. }) => worker_handle.abort(),
            None => {}
        }
    }
}