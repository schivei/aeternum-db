using System.Text.Json.Serialization;

namespace AeternumDB.Core.Types;

/// <summary>
/// A SQL column value.
/// </summary>
[JsonDerivedType(typeof(Null), "null")]
[JsonDerivedType(typeof(Boolean), "boolean")]
[JsonDerivedType(typeof(Integer), "integer")]
[JsonDerivedType(typeof(Float), "float")]
[JsonDerivedType(typeof(Text), "text")]
[JsonDerivedType(typeof(Bytes), "bytes")]
[JsonDerivedType(typeof(Array), "array")]
public abstract class DbValue : IEquatable<DbValue>
{
    private DbValue() { }

    public sealed class Null : DbValue
    {
        public static readonly Null Instance = new();
        private Null() { }
        public override string ToString() => "NULL";
        public override bool Equals(DbValue? other) => other is Null;
        public override int GetHashCode() => 0;
    }

    public sealed class Boolean(bool value) : DbValue
    {
        public bool Value { get; } = value;
        public override string ToString() => Value ? "TRUE" : "FALSE";
        public override bool Equals(DbValue? other) => other is Boolean b && b.Value == Value;
        public override int GetHashCode() => Value.GetHashCode();
    }

    public sealed class Integer(long value) : DbValue
    {
        public long Value { get; } = value;
        public override string ToString() => Value.ToString();
        public override bool Equals(DbValue? other) => other is Integer i && i.Value == Value;
        public override int GetHashCode() => Value.GetHashCode();
    }

    public sealed class Float(double value) : DbValue
    {
        public double Value { get; } = value;
        public override string ToString() => Value.ToString("G17");
        public override bool Equals(DbValue? other) => other is Float f && f.Value == Value;
        public override int GetHashCode() => Value.GetHashCode();
    }

    public sealed class Text(string value) : DbValue
    {
        public string Value { get; } = value;
        public override string ToString() => Value;
        public override bool Equals(DbValue? other) => other is Text t && t.Value == Value;
        public override int GetHashCode() => Value.GetHashCode(StringComparison.Ordinal);
    }

    public sealed class Bytes(byte[] value) : DbValue
    {
        public byte[] Value { get; } = value;
        public override string ToString() => $"<{Value.Length} bytes>";
        public override bool Equals(DbValue? other) =>
            other is Bytes b && b.Value.AsSpan().SequenceEqual(Value);
        public override int GetHashCode() => Value.Length;
    }

    public sealed class Array(DbValue[] items) : DbValue
    {
        public DbValue[] Items { get; } = items;
        public override string ToString() => $"[{string.Join(", ", (IEnumerable<DbValue>)Items)}]";
        public override bool Equals(DbValue? other) =>
            other is Array a && a.Items.SequenceEqual(Items);
        public override int GetHashCode() => Items.Length;
    }

    public bool IsNull => this is Null;
    public abstract bool Equals(DbValue? other);
    public override bool Equals(object? obj) => obj is DbValue v && Equals(v);
    public abstract override int GetHashCode();
}
