#![allow(dead_code, unused_imports)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datalog::babyflow::{Operator, Query};
use pprof::criterion::{Output, PProfProfiler};
use std::sync::mpsc::channel;
use std::thread::{self, sleep};
use std::time::Duration;
use timely::dataflow::operators::{Concat, Filter, Inspect, Map, ToStream};

const NUM_OPS: usize = 20;
const NUM_INTS: usize = 1_000_000;

fn benchmark_babyflow(c: &mut Criterion) {
    c.bench_function("babyflow", |b| {
        b.iter(|| {
            let mut q = Query::new();

            let mut op = q.source(move |send| {
                for i in 0..NUM_INTS {
                    send.push(i);
                }
            });

            for _ in 0..NUM_OPS {
                let op1 = op.clone().filter(|x| x % 2 == 0);
                let op2 = op.filter(|x| x % 2 == 1);
                op = op1.union(op2)
            }

            op.sink(|i| {
                black_box(i);
            });

            (*q.df).borrow_mut().run();
        })
    });
}

fn benchmark_timely(c: &mut Criterion) {
    c.bench_function("timely", |b| {
        b.iter(|| {
            timely::example(|scope| {
                let mut op = (0..NUM_INTS).to_stream(scope);
                for _ in 0..NUM_OPS {
                    let op1 = op.filter(|i| i % 2 == 0);
                    let op2 = op.filter(|i| i % 2 == 1);
                    op = op1.concat(&op2);
                }

                op.inspect(|i| {
                    black_box(i);
                });
            });
        })
    });
}

// criterion_group!(
//     name = fork_join_dataflow;
//     config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
//     targets = benchmark_babyflow
// );
// criterion_group!(fork_join_dataflow, benchmark_timely,);
criterion_group!(fork_join_dataflow, benchmark_babyflow, benchmark_timely);
criterion_main!(fork_join_dataflow);
