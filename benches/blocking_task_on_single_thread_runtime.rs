mod common;

use common::*;
use criterion::{criterion_group, criterion_main, Criterion};


fn sequential_executor_on_single_thread_runtime(c: &mut Criterion) {
    blocking_task_with_sequential_executor(c, single_rt(), "blocking_task_with_sequential_executor_on_single_thread_runtime");
}

fn tokio_spawn_mutex_on_single_thread_runtime(c: &mut Criterion) {
    blocking_task_with_tokio_spawn_mutex(c, single_rt(), "blocking_task_with_tokio_spawn_mutex_on_single_thread_runtime");
}

criterion_group!(
    bench_main,
    sequential_executor_on_single_thread_runtime,
    tokio_spawn_mutex_on_single_thread_runtime,
);

criterion_main!(bench_main);