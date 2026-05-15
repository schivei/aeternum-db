using AeternumDB.PoC.Safe.Benchmarks;
using BenchmarkDotNet.Running;

// NativeAOT-compatible entry point: BenchmarkSwitcher resolves types at compile time
// when the assembly is trimmed/AOT-compiled.
BenchmarkSwitcher
    .FromAssembly(typeof(StorageBenchmarks).Assembly)
    .Run(args);
