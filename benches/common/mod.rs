#![allow(unused)]

use async_sequential::JoinQueue;
use criterion::Criterion;
use tokio::runtime::Runtime;
use std::hint::black_box;
use std::sync::Arc;
use tokio::sync::Mutex;


pub fn multi_rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .build()
        .unwrap()
}

pub fn single_rt() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}


const TASK_COUNTS: &[usize] = &[100, 1_000, 10_000, 100_000];
const SAMPLE_SIZE: usize = 30;

pub fn async_task_with_sequential_executor(c: &mut Criterion, runtime: Runtime, name: &str) {
    let mut group = c.benchmark_group(name);
    group.sample_size(SAMPLE_SIZE);

    for task_count in TASK_COUNTS.iter().cloned() {
        group.bench_function(task_count.to_string(), |b| {
            b.iter(|| {
                runtime.block_on(async {
                    let executor = JoinQueue::new(0);

                    for _ in 0..task_count {
                        executor.spawn(|state| Box::pin(async {
                            black_box({
                                *state += 1;
                                *state
                            });
                        }));
                    }

                    executor.join().await;
                });
            });
        });
    }

    group.finish();
}

pub fn async_task_with_tokio_spawn_mutex(c: &mut Criterion, runtime: Runtime, name: &str) {
    let mut group = c.benchmark_group(name);
    group.sample_size(SAMPLE_SIZE);

    for task_count in TASK_COUNTS.iter().cloned() {
        group.bench_function(task_count.to_string(), |b| {
            b.iter(|| {
                runtime.block_on(async {
                    let state = Arc::new(Mutex::new(0));
                    let mut handles = Vec::with_capacity(task_count);

                    for _ in 0..task_count {
                        let state = state.clone();
                        handles.push(tokio::task::spawn(async move {
                            let mut state = state.lock().await;
                            black_box({
                                *state += 1;
                                *state
                            });
                        }));
                    }

                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
            });
        });
    }

    group.finish();
}

pub fn blocking_task_with_sequential_executor(c: &mut Criterion, runtime: Runtime, name: &str) {
    let mut group = c.benchmark_group(name);
    group.sample_size(SAMPLE_SIZE);

    for task_count in TASK_COUNTS.iter().cloned() {
        group.bench_function(task_count.to_string(), |b| {
            b.iter(|| {
                runtime.block_on(async {
                    let executor = JoinQueue::new(0);

                    for _ in 0..task_count {
                        executor.spawn_blocking(|state| {
                            black_box({
                                *state += 1;
                                *state
                            });
                        });
                    }

                    executor.join().await;
                });
            });
        });
    }

    group.finish();
}

pub fn blocking_task_with_tokio_spawn_mutex(c: &mut Criterion, runtime: Runtime, name: &str) {
    let mut group = c.benchmark_group(name);
    group.sample_size(SAMPLE_SIZE);

    for task_count in TASK_COUNTS.iter().cloned() {
        group.bench_function(task_count.to_string(), |b| {
            b.iter(|| {
                runtime.block_on(async {
                    let state = Arc::new(Mutex::new(0));
                    let mut handles = Vec::with_capacity(task_count);

                    for _ in 0..task_count {
                        let state = state.clone();
                        handles.push(tokio::task::spawn_blocking(move || {
                            let mut state = state.blocking_lock();
                            black_box({
                                *state += 1;
                                *state
                            });
                        }));
                    }

                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
            });
        });
    }

    group.finish();
}