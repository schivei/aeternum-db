using System.Text.Json.Serialization;

namespace AeternumDB.Core.Types;

/// <summary>
/// A SQL column value. Discriminated union matching Rust's Value enum.
/// </summary>
[JsonDerivedType(typeof(Null), "null")]
[JsonDerivedType(typeof(Boolean), "boolean")]
[JsonDerivedType(typeof(Integer), "integer")]
[JsonDerivedType(typeof(Float), "float")]
[JsonDerivedType(typeof(Text), "text")]
[JsonDerivedType(typeof(Bytes), "bytes")]
[JsonDerivedType(typeof(Array), "array")]
[JsonDerivedType(typeof(Decimal), "decimal")]
[JsonDerivedType(typeof(Json), "json")]
public abstract class DbValue : IEquatable<DbValue>
{
    private DbValue() { }

    public sealed class Null : DbValue
    {
        public static readonly Null Instance = new();
        private Null() { }
        public override string ToString() => "NULL";
        public override bool Equals(DbValue? other) => other is Null;
        public override bool Equals(object? obj) => obj is Null;
        public override int GetHashCode() => 0;
        public override string? AsString() => null;
    }

    public sealed class Boolean(bool value) : DbValue
    {
        public bool Value { get; } = value;
        public override string ToString() => Value ? "TRUE" : "FALSE";
        public override bool Equals(DbValue? other) => other is Boolean b && b.Value == Value;
        public override bool Equals(object? obj) => obj is Boolean b && b.Value == Value;
        public override int GetHashCode() => Value.GetHashCode();
        public override bool? AsBool() => Value;
    }

    public sealed class Integer(long value) : DbValue
    {
        public long Value { get; } = value;
        public override string ToString() => Value.ToString();
        public override bool Equals(DbValue? other) => other is Integer i && i.Value == Value;
        public override bool Equals(object? obj) => obj is Integer i && i.Value == Value;
        public override int GetHashCode() => Value.GetHashCode();
        public override long? AsInteger() => Value;
        public override double? AsFloat() => (double)Value;
    }

    public sealed class Float(double value) : DbValue
    {
        public double Value { get; } = value;
        public override string ToString() => Value.ToString("G17");
        public override bool Equals(DbValue? other) =>
            other is Float f && f.Value.Equals(Value);
        public override bool Equals(object? obj) =>
            obj is Float f && f.Value.Equals(Value);
        public override int GetHashCode() => BitConverter.DoubleToInt64Bits(Value).GetHashCode();
        public override double? AsFloat() => Value;
    }

    public sealed class Text(string value) : DbValue
    {
        public string Value { get; } = value;
        public override string ToString() => Value;
        public override bool Equals(DbValue? other) => other is Text t && t.Value == Value;
        public override bool Equals(object? obj) => obj is Text t && t.Value == Value;
        public override int GetHashCode() => Value.GetHashCode(StringComparison.Ordinal);
        public override string? AsString() => Value;
    }

    public sealed class Bytes(byte[] value) : DbValue
    {
        public byte[] Value { get; } = value;
        public override string ToString() => $"<{Value.Length} bytes>";
        public override bool Equals(DbValue? other) =>
            other is Bytes b && b.Value.AsSpan().SequenceEqual(Value);
        public override bool Equals(object? obj) =>
            obj is Bytes b && b.Value.AsSpan().SequenceEqual(Value);
        public override int GetHashCode() => Value.Length;
    }

    public sealed class Array(DbValue[] items) : DbValue
    {
        public DbValue[] Items { get; } = items;
        public override string ToString() => $"[{string.Join(", ", (IEnumerable<DbValue>)Items)}]";
        public override bool Equals(DbValue? other) =>
            other is Array a && a.Items.SequenceEqual(Items);
        public override bool Equals(object? obj) =>
            obj is Array a && a.Items.SequenceEqual(Items);
        public override int GetHashCode() => Items.Length;
        public override DbValue[]? AsArray() => Items;
    }

    /// <summary>Exact decimal value (maps to Rust's Decimal variant).</summary>
    public sealed class Decimal(decimal value) : DbValue
    {
        public decimal Value { get; } = value;
        public override string ToString() => Value.ToString();
        public override bool Equals(DbValue? other) => other is Decimal d && d.Value == Value;
        public override bool Equals(object? obj) => obj is Decimal d && d.Value == Value;
        public override int GetHashCode() => Value.GetHashCode();
    }

    /// <summary>JSON value stored as raw JSON string (maps to Rust's Json variant).</summary>
    public sealed class Json(string value) : DbValue
    {
        public string Value { get; } = value;
        public override string ToString() => Value;
        public override bool Equals(DbValue? other) => other is Json j && j.Value == Value;
        public override bool Equals(object? obj) => obj is Json j && j.Value == Value;
        public override int GetHashCode() => Value.GetHashCode(StringComparison.Ordinal);
    }

    public bool IsNull => this is Null;
    public abstract bool Equals(DbValue? other);
    public abstract override bool Equals(object? obj);
    public abstract override int GetHashCode();

    // ── Conversion helpers ────────────────────────────────────────────────

    /// <summary>Returns the integer value, or null if not an Integer or Null.</summary>
    public virtual long? AsInteger() => null;

    /// <summary>Returns the float value (Integer is coerced), or null.</summary>
    public virtual double? AsFloat() => null;

    /// <summary>Returns the string representation, or null for Null.</summary>
    public virtual string? AsString() => ToString();

    /// <summary>Returns the boolean value, or null if not a Boolean or Null.</summary>
    public virtual bool? AsBool() => null;

    /// <summary>Returns the array items, or null if not an Array or Null.</summary>
    public virtual DbValue[]? AsArray() => null;
}
