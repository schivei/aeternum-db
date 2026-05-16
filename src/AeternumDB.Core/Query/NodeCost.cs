namespace AeternumDB.Core.Query;

/// <summary>Cost annotation for a physical plan node.</summary>
public sealed class NodeCost(double rows, double cpu, double io)
{
    /// <summary>Estimated number of output rows.</summary>
    public double Rows { get; } = rows;

    /// <summary>Estimated CPU cost.</summary>
    public double Cpu { get; } = cpu;

    /// <summary>Estimated I/O cost.</summary>
    public double Io { get; } = io;

    /// <summary>A zero-cost sentinel instance.</summary>
    public static readonly NodeCost Zero = new(0, 0, 0);
}
