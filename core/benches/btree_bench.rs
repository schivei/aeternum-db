//! Criterion benchmarks for the B-tree index.
//!
//! Run with: `cargo bench --bench btree_bench`

use aeternumdb_core::index::btree::{BTree, BTreeConfig};
use aeternumdb_core::storage::{StorageConfig, StorageEngine};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

const PAGE_SIZE: usize = 8192;
const POOL_SIZE: usize = 4096;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn make_tree(fanout: usize) -> (BTree<i64, String>, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    let engine = StorageEngine::new(StorageConfig {
        data_path: tmp.path().to_path_buf(),
        buffer_pool_size: POOL_SIZE,
        page_size: PAGE_SIZE,
    })
    .await
    .unwrap();
    let tree = BTree::<i64, String>::new(Arc::new(engine), BTreeConfig { fanout })
        .await
        .unwrap();
    (tree, tmp)
}

// ── Sequential insert ─────────────────────────────────────────────────────────

fn bench_sequential_insert(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("btree_sequential_insert");

    for &n in &[100usize, 500, 1000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || rt.block_on(make_tree(100)),
                |(tree, _tmp)| {
                    rt.block_on(async {
                        for i in 0..n as i64 {
                            tree.insert(i, i.to_string()).await.unwrap();
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ── Random insert ─────────────────────────────────────────────────────────────

fn bench_random_insert(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("btree_random_insert");

    for &n in &[100usize, 500, 1000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            // Pre-compute shuffled keys outside the timed section.
            let mut keys: Vec<i64> = (0..n as i64).collect();
            for i in 0..keys.len() {
                let j = (i * 17 + 5) % keys.len();
                keys.swap(i, j);
            }
            let keys = Arc::new(keys);

            b.iter_batched(
                || rt.block_on(make_tree(100)),
                |(tree, _tmp)| {
                    let keys = keys.clone();
                    rt.block_on(async move {
                        for &k in keys.iter() {
                            tree.insert(k, k.to_string()).await.unwrap();
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ── Point query ───────────────────────────────────────────────────────────────

fn bench_point_query(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("btree_point_query");

    for &n in &[100usize, 500, 1000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let (tree, _tmp) = rt.block_on(make_tree(100));
            rt.block_on(async {
                for i in 0..n as i64 {
                    tree.insert(i, i.to_string()).await.unwrap();
                }
            });
            let tree = Arc::new(tree);

            b.iter(|| {
                rt.block_on(async {
                    for i in 0..n as i64 {
                        let _ = tree.search(&i).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

// ── Range scan ────────────────────────────────────────────────────────────────

fn bench_range_scan(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("btree_range_scan");

    for &n in &[100usize, 500, 1000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let (tree, _tmp) = rt.block_on(make_tree(100));
            rt.block_on(async {
                for i in 0..n as i64 {
                    tree.insert(i, i.to_string()).await.unwrap();
                }
            });
            let tree = Arc::new(tree);
            let end = n as i64;

            b.iter(|| {
                rt.block_on(async {
                    let iter = tree.range(0i64..end).await.unwrap();
                    let count = iter.count();
                    assert_eq!(count, n);
                });
            });
        });
    }
    group.finish();
}

// ── Delete ────────────────────────────────────────────────────────────────────

fn bench_delete(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("btree_delete");

    for &n in &[100usize, 500] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let (tree, tmp) = rt.block_on(make_tree(100));
                    rt.block_on(async {
                        for i in 0..n as i64 {
                            tree.insert(i, i.to_string()).await.unwrap();
                        }
                    });
                    (tree, tmp)
                },
                |(tree, _tmp)| {
                    rt.block_on(async {
                        for i in 0..n as i64 {
                            tree.delete(&i).await.unwrap();
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ── Bulk load ─────────────────────────────────────────────────────────────────

fn bench_bulk_load(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("btree_bulk_load");

    for &n in &[100usize, 500, 1000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let entries: Vec<(i64, String)> = (0..n as i64).map(|i| (i, i.to_string())).collect();
            let entries = Arc::new(entries);

            b.iter_batched(
                || rt.block_on(make_tree(100)),
                |(tree, _tmp)| {
                    let entries = entries.as_ref().clone();
                    rt.block_on(async move {
                        tree.bulk_load(entries).await.unwrap();
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_insert,
    bench_random_insert,
    bench_point_query,
    bench_range_scan,
    bench_delete,
    bench_bulk_load,
);
criterion_main!(benches);
