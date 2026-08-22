Note: **I’m using a translation tool, so some expressions may be awkward or inaccurate.**

# Overview

Provides an executor for running asynchronous and blocking tasks sequentially on shared mutable state.

This crate requires the `tokio` async runtime.

# Usage

```rust
#[tokio::main]
async fn main() {
    let executor = async_sequential::Executor::new(Vec::new());

    executor.spawn(move |state: &mut Vec<u64>| Box::pin(async move {
        state.push(identity(0).await);
    }));

    executor.spawn_blocking(move |state| {
        state.push(1);
    });

    let task_result = executor.execute(move |state| Box::pin(async move {
        state.push(identity(2).await);
        "hello"
    })).await;
    assert_eq!(task_result, "hello");

    let task_result = executor.execute_blocking(move |state| {
        state.push(3);
        "world"
    }).await;
    assert_eq!(task_result, "world");

    let result = executor.join().await;
    assert_eq!(result, vec![0, 1, 2, 3]);
}

async fn identity(v: u64) -> u64 {
    v
}
```

# License

Licensed under either of

 * MIT license
 * Apache License (Version 2.0)

at your option.