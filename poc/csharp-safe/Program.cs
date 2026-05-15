using AeternumDB.PoC.Safe.Benchmarks;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.DependencyInjection;
using AeternumDB.PoC.Safe.DependencyInjection;

if (args.Length > 0 && args[0] == "--benchmark")
{
    BenchmarkSwitcher.FromAssembly(typeof(StorageBenchmarks).Assembly).Run(args[1..]);
    return;
}

var services = new ServiceCollection();
services.AddGenDIServices();
var provider = services.BuildServiceProvider();

Console.WriteLine("AeternumDB C# Safe PoC — smoke test");
Console.WriteLine("GenDI container built. Registered services:");
foreach (var sd in services)
    Console.WriteLine($"  {sd.ServiceType.Name} → {sd.ImplementationType?.Name ?? "(factory)"}");
Console.WriteLine();
Console.WriteLine("Run with --benchmark to execute BenchmarkDotNet suites.");
