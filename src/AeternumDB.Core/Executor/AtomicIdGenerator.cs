namespace AeternumDB.Core.Executor;

/// <summary>Thread-safe atomic counter-based ID generator.</summary>
public sealed class AtomicIdGenerator : IObjIdGenerator
{
    private long _counter;

    public AtomicIdGenerator(long start = 1) => _counter = start - 1;

    public long NextId() => Interlocked.Increment(ref _counter);
}
