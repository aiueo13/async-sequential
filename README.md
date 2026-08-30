Note: **I’m using a translation tool, so some expressions may be awkward or inaccurate.**

# Overview

Provides a worker for running asynchronous and blocking tasks sequentially on shared mutable state, allowing the final state to be obtained after all tasks complete.

This crate requires the `tokio` async runtime.

# Usage

```rust
use std::{thread, time::Duration};
use tokio::time::sleep;
 
#[tokio::main]
async fn main() {
    // Spawn a worker with the given state
    let worker = async_sequential::spawn_worker(Vec::new());

    // Spawn a task onto the worker
    worker.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
        sleep(Duration::from_secs(1)).await;
        state.push(1);
    }));

    // A spawner can be used to spawn tasks onto the worker
    // from another thread or Tokio task.
    let spawner = worker.spawner();
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
 
        // Drop the spawner to allow the worker to complete.
        drop(spawner);
    });

    // Wait for all tasks to complete.
    // NOTE: This does not complete as long as any spawner remains alive.
    let result = worker.join().await.unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}
```

# License

Licensed under either of

 * MIT license
 * Apache License (Version 2.0)

at your option.