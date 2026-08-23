use std::sync::Arc;


pub enum WorkerSendError {
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>,
    },
}

pub enum WorkerTaskSenderSendError {
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>,
    },
    WorkerAborted,
    WorkerJoined,
    WorkerCancelled,
}
