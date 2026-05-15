using AeternumDB.PoC.Unsafe.Benchmarks;
using BenchmarkDotNet.Running;

// NativeAOT-compatible entry point.
BenchmarkSwitcher
    .FromAssembly(typeof(StorageBenchmarks).Assembly)
    .Run(args);
