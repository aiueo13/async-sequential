Note: **I’m using a translation tool, so some expressions may be awkward or inaccurate.**

# Overview

Provides an executor for running asynchronous and blocking tasks sequentially on shared mutable state.

This crate requires the `tokio` async runtime.

# Usage

```rust
use std::{thread, time::Duration};
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let executor = async_sequential::Executor::new(Vec::new());

    executor.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
        sleep(Duration::from_secs(1)).await;
        state.push(1);
    }));

    let spawner = executor.spawner();
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
    });

    // Wait for all tasks to complete.
    // NOTE: This does not complete as long as `spawner` has not been dropped.
    let result = executor.join().await;
    assert_eq!(result, vec![1, 2, 3]);
}
```

# License

Licensed under either of

 * MIT license
 * Apache License (Version 2.0)

at your option.