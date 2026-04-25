//! Criterion benchmarks for the storage engine.
//!
//! Run with: `cargo bench --bench storage_bench`

use aeternumdb_core::storage::{StorageConfig, StorageEngine};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

const PAGE_SIZE: usize = 8192;
const POOL_SIZE: usize = 1024;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn make_engine(pool_size: usize) -> (StorageEngine, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    let engine = StorageEngine::new(StorageConfig {
        data_path: tmp.path().to_path_buf(),
        buffer_pool_size: pool_size,
        page_size: PAGE_SIZE,
    })
    .await
    .unwrap();
    (engine, tmp)
}

// ── Sequential writes ─────────────────────────────────────────────────────────

fn xor_shuffle_index(current: usize, n: usize) -> usize {
    (current ^ (current >> 3) ^ 0xABCD) % n
}

fn is_write_step(step: usize) -> bool {
    step % 5 == 0
}

fn bench_sequential_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_write");
    let payload = vec![0xAAu8; 64];

    for &n in &[100usize, 500, 1000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let rt = rt();
                rt.block_on(async {
                    let (engine, _tmp) = make_engine(POOL_SIZE).await;
                    for _ in 0..n {
                        let id = engine.allocate_page().await.unwrap();
                        engine.write_page_data(id, 0, &payload).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

// ── Random reads ──────────────────────────────────────────────────────────────

fn bench_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_read");

    for &n in &[100usize, 500, 1000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let rt = rt();
                rt.block_on(async {
                    let (engine, _tmp) = make_engine(POOL_SIZE).await;
                    let mut ids = Vec::with_capacity(n);
                    let payload = vec![0xBBu8; 64];
                    for _ in 0..n {
                        let id = engine.allocate_page().await.unwrap();
                        engine.write_page_data(id, 0, &payload).await.unwrap();
                        ids.push(id);
                    }
                    let mut idx = 0usize;
                    for _ in 0..n {
                        idx = xor_shuffle_index(idx, n);
                        let _ = engine.read_page_data(ids[idx], 0, 64).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

// ── Mixed workload (80 % read, 20 % write) ────────────────────────────────────

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_80r_20w");
    let payload = vec![0xCCu8; 64];

    for &n in &[100usize, 500] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let rt = rt();
                rt.block_on(async {
                    let (engine, _tmp) = make_engine(POOL_SIZE).await;
                    let mut ids: Vec<u64> = Vec::with_capacity(n);
                    for _ in 0..n {
                        let id = engine.allocate_page().await.unwrap();
                        engine.write_page_data(id, 0, &payload).await.unwrap();
                        ids.push(id);
                    }
                    for i in 0..n {
                        if is_write_step(i) {
                            engine
                                .write_page_data(ids[i % ids.len()], 0, &payload)
                                .await
                                .unwrap();
                        } else {
                            let _ = engine
                                .read_page_data(ids[i % ids.len()], 0, 64)
                                .await
                                .unwrap();
                        }
                    }
                });
            });
        });
    }
    group.finish();
}

// ── Buffer-hit reads (all pages fit in pool) ──────────────────────────────────

fn bench_buffer_hit_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_hit_read");

    for &n in &[50usize, 100] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let rt = rt();
            let (engine, _tmp, ids) = rt.block_on(async {
                let (engine, tmp) = make_engine(POOL_SIZE).await;
                let payload = vec![0xDDu8; 64];
                let mut ids = Vec::with_capacity(n);
                for _ in 0..n {
                    let id = engine.allocate_page().await.unwrap();
                    engine.write_page_data(id, 0, &payload).await.unwrap();
                    ids.push(id);
                }
                (engine, tmp, ids)
            });

            b.iter(|| {
                let rt = rt();
                rt.block_on(async {
                    for &id in &ids {
                        let _ = engine.read_page_data(id, 0, 64).await.unwrap();
                    }
                });
            });

            drop(_tmp);
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_write,
    bench_random_read,
    bench_mixed_workload,
    bench_buffer_hit_latency,
);
criterion_main!(benches);
