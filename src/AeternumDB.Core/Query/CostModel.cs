namespace AeternumDB.Core.Query;

public sealed class CostModel(double ioCostFactor = 1.0, double cpuCostFactor = 0.01, double networkCostFactor = 10.0)
{
    public double IoCostFactor { get; } = ioCostFactor;
    public double CpuCostFactor { get; } = cpuCostFactor;
    public double NetworkCostFactor { get; } = networkCostFactor;

    public double EstimateScanCost(TableStats stats) =>
        stats.NumPages * IoCostFactor + stats.NumRows * CpuCostFactor;

    public double EstimateFilterCost(int inputRows, double selectivity)
    {
        var sel = Math.Clamp(selectivity, 0.0, 1.0);
        return inputRows * CpuCostFactor * (1.0 + sel);
    }

    public double EstimateNestedLoopCost(int leftRows, int rightRows) =>
        leftRows * (double)rightRows * CpuCostFactor;

    public double EstimateHashJoinCost(int leftRows, int rightRows) =>
        leftRows * CpuCostFactor * 1.5 + rightRows * CpuCostFactor;

    public double EstimateSortCost(int rows)
    {
        if (rows <= 1) return 0.0;
        var n = (double)rows;
        return n * Math.Log2(n) * CpuCostFactor;
    }

    public double EstimateAggregateCost(int inputRows, int groups) =>
        inputRows * CpuCostFactor + groups * CpuCostFactor * 2.0;

    public static int EstimatedRows(int inputRows, double selectivity)
    {
        var sel = Math.Clamp(selectivity, 0.0, 1.0);
        var rows = (int)Math.Ceiling(inputRows * sel);
        return Math.Max(1, rows);
    }
}
