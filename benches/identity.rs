#![allow(dead_code, unused_imports)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datalog::babyflow::Query;
use std::sync::mpsc::channel;
use std::thread::{self, sleep};
use std::time::Duration;
use timely::dataflow::operators::{Inspect, Map, ToStream};

const NUM_OPS: usize = 20;
const NUM_INTS: usize = 1_000_000;

// This benchmark runs babyflow which more-or-less just copies the data directly
// between the operators, but with some extra overhead.
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
                op = op.map(black_box);
            }

            op.sink(|i| {
                black_box(i);
            });

            (*q.df).borrow_mut().run();
        })
    });
}

fn benchmark_pipeline(c: &mut Criterion) {
    c.bench_function("pipeline", |b| {
        b.iter(|| {
            let (input, mut output) = channel();

            for _ in 0..NUM_OPS {
                let (tx, mut rx) = channel();
                std::mem::swap(&mut output, &mut rx);
                thread::spawn(move || {
                    for elt in rx {
                        tx.send(elt).unwrap();
                    }
                });
            }

            for i in 0..NUM_INTS {
                input.send(i).unwrap();
            }
            drop(input);
            for elt in output {
                black_box(elt);
            }
        });
    });
}

// This benchmark just copies around a bunch of data with basically zero
// overhead, so this should theoretically be the fastest achievable (with a
// single thread).
fn benchmark_raw_copy(c: &mut Criterion) {
    c.bench_function("raw copy", |b| {
        b.iter(|| {
            let mut data: Vec<_> = (0..NUM_INTS).collect();
            let mut next = Vec::new();

            for _ in 0..NUM_OPS {
                next.extend(data.drain(..));
                std::mem::swap(&mut data, &mut next);
            }

            for elt in data {
                black_box(elt);
            }
        })
    });
}

fn benchmark_iter(c: &mut Criterion) {
    c.bench_function("iter", |b| {
        b.iter(|| {
            let data: Vec<_> = (0..NUM_INTS).collect();

            let iter = data.into_iter();

            ///// MAGIC NUMBER!!!!!!!! is NUM_OPS
            seq_macro::seq!(_ in 0..20 {
                let iter = iter.map(black_box);
            });

            let data: Vec<_> = iter.collect();

            for elt in data {
                black_box(elt);
            }
        });
    });
}

fn benchmark_iter_collect(c: &mut Criterion) {
    c.bench_function("iter-collect", |b| {
        b.iter(|| {
            let mut data: Vec<_> = (0..NUM_INTS).collect();

            for _ in 0..NUM_OPS {
                let iter = data.into_iter();
                let iter = iter.map(black_box);
                data = iter.collect();
            }

            for elt in data {
                black_box(elt);
            }
        });
    });
}

async fn benchmark_spinach(num_ints: usize) {
    use spinach::comp::Comp;

    type MyLatRepr = spinach::lattice::set_union::SetUnionRepr<spinach::tag::VEC, usize>;
    let op = <spinach::op::OnceOp<MyLatRepr>>::new((0..num_ints).collect());

    struct MyMorphism();
    impl spinach::func::unary::Morphism for MyMorphism {
        type InLatRepr = MyLatRepr;
        type OutLatRepr = MyLatRepr;
        fn call<Y: spinach::hide::Qualifier>(
            &self,
            item: spinach::hide::Hide<Y, Self::InLatRepr>,
        ) -> spinach::hide::Hide<Y, Self::OutLatRepr> {
            item.map(black_box)
        }
    }

    ///// MAGIC NUMBER!!!!!!!! is NUM_OPS
    seq_macro::seq!(N in 0..20 {
        let op = spinach::op::MorphismOp::new(op, MyMorphism());
    });

    let comp = spinach::comp::NullComp::new(op);
    spinach::comp::CompExt::run(&comp).await.unwrap_err();
}

fn criterion_spinach(c: &mut Criterion) {
    c.bench_function("spinach", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| benchmark_spinach(NUM_INTS));
    });
}

fn benchmark_timely(c: &mut Criterion) {
    c.bench_function("timely", |b| {
        b.iter(|| {
            timely::example(|scope| {
                let mut op = (0..NUM_INTS).to_stream(scope);
                for _ in 0..NUM_OPS {
                    op = op.map(black_box)
                }

                op.inspect(|i| {
                    black_box(i);
                });
            });
        })
    });
}

criterion_group!(
    identity_dataflow,
    benchmark_timely,
    benchmark_babyflow,
    criterion_spinach,
    benchmark_pipeline,
    benchmark_iter,
    benchmark_iter_collect,
    benchmark_raw_copy,
);
criterion_main!(identity_dataflow);
