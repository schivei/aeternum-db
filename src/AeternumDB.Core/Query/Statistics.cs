namespace AeternumDB.Core.Query;

using AeternumDB.Core.Sql;

public sealed class HistogramBucket(SqlValue lowerBound, SqlValue upperBound, int count)
{
    public SqlValue LowerBound { get; } = lowerBound;
    public SqlValue UpperBound { get; } = upperBound;
    public int Count { get; } = count;
}

public sealed class Histogram(List<HistogramBucket> buckets)
{
    public List<HistogramBucket> Buckets { get; } = buckets;

    public double SelectivityRange(SqlValue low, SqlValue high)
    {
        if (Buckets.Count == 0) return 0.1;
        var total = Buckets.Sum(b => Math.Max(0, b.Count));
        if (total == 0) return 0.1;

        var matching = Buckets
            .Where(b => ValueLte(b.LowerBound, high) && ValueLt(low, b.UpperBound))
            .Sum(b => Math.Max(0, b.Count));

        return (double)matching / total;
    }

    private static bool ValueLte(SqlValue a, SqlValue b) =>
        (a, b) switch
        {
            (SqlValue.Integer x, SqlValue.Integer y) => x.Value <= y.Value,
            (SqlValue.Float x, SqlValue.Float y) => x.Value <= y.Value,
            (SqlValue.SqlString x, SqlValue.SqlString y) => string.CompareOrdinal(x.Value, y.Value) <= 0,
            _ => true
        };

    private static bool ValueLt(SqlValue a, SqlValue b) =>
        (a, b) switch
        {
            (SqlValue.Integer x, SqlValue.Integer y) => x.Value < y.Value,
            (SqlValue.Float x, SqlValue.Float y) => x.Value < y.Value,
            (SqlValue.SqlString x, SqlValue.SqlString y) => string.CompareOrdinal(x.Value, y.Value) < 0,
            _ => true
        };
}

public sealed class ColumnStats(
    string columnName,
    int numDistinct,
    int numNulls,
    SqlValue? minValue,
    SqlValue? maxValue,
    Histogram? histogram,
    int? innerCount = null)
{
    public string ColumnName { get; } = columnName;
    public int NumDistinct { get; } = numDistinct;
    public int NumNulls { get; } = numNulls;
    public SqlValue? MinValue { get; } = minValue;
    public SqlValue? MaxValue { get; } = maxValue;
    public Histogram? Histogram { get; } = histogram;
    public int? InnerCount { get; } = innerCount;

    public double SelectivityEq() => NumDistinct > 0 ? 1.0 / NumDistinct : 0.1;
}

public sealed class TableStats(string tableName)
{
    public string TableName { get; } = tableName.ToLowerInvariant();
    public int NumRows { get; set; } = 1000;
    public int NumPages { get; set; } = 10;
    public int AvgRowSize { get; set; } = 128;
    public bool IsFlat { get; set; }
    public Dictionary<string, ColumnStats> ColumnStats { get; } = new(StringComparer.OrdinalIgnoreCase);

    public ColumnStats? Column(string name) =>
        ColumnStats.TryGetValue(name, out var c) ? c : null;
}

public sealed class StatisticsRegistry
{
    private readonly Dictionary<string, TableStats> _tables = new(StringComparer.OrdinalIgnoreCase);

    public void Add(TableStats stats) => _tables[stats.TableName.ToLowerInvariant()] = stats;

    public TableStats Get(string tableName) =>
        _tables.TryGetValue(tableName.ToLowerInvariant(), out var s)
            ? s
            : new TableStats(tableName);
}
