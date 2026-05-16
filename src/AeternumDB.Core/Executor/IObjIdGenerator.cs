namespace AeternumDB.Core.Executor;

/// <summary>Generator for unique object IDs.</summary>
public interface IObjIdGenerator
{
    long NextId();
}
