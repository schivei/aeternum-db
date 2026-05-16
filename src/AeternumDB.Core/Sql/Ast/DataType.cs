namespace AeternumDB.Core.Sql.Ast;

/// <summary>Discriminated union representing all SQL column data types supported by AeternumDB.</summary>
public abstract class DataType
{
    private DataType() { }

    #region Integer Types

    /// <summary>Signed 32-bit integer.</summary>
    public sealed class Integer : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Integer Instance = new();

        private Integer() { }

        /// <inheritdoc/>
        public override string ToString() => "INTEGER";
    }

    /// <summary>Unsigned 32-bit integer.</summary>
    public sealed class UnsignedInt : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly UnsignedInt Instance = new();

        private UnsignedInt() { }

        /// <inheritdoc/>
        public override string ToString() => "INTEGER UNSIGNED";
    }

    /// <summary>Signed 8-bit integer.</summary>
    public sealed class TinyInt : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly TinyInt Instance = new();

        private TinyInt() { }

        /// <inheritdoc/>
        public override string ToString() => "TINYINT";
    }

    /// <summary>Unsigned 8-bit integer.</summary>
    public sealed class UnsignedTinyInt : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly UnsignedTinyInt Instance = new();

        private UnsignedTinyInt() { }

        /// <inheritdoc/>
        public override string ToString() => "TINYINT UNSIGNED";
    }

    /// <summary>Signed 16-bit integer.</summary>
    public sealed class SmallInt : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly SmallInt Instance = new();

        private SmallInt() { }

        /// <inheritdoc/>
        public override string ToString() => "SMALLINT";
    }

    /// <summary>Unsigned 16-bit integer.</summary>
    public sealed class UnsignedSmallInt : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly UnsignedSmallInt Instance = new();

        private UnsignedSmallInt() { }

        /// <inheritdoc/>
        public override string ToString() => "SMALLINT UNSIGNED";
    }

    /// <summary>Signed 24-bit integer.</summary>
    public sealed class MediumInt : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly MediumInt Instance = new();

        private MediumInt() { }

        /// <inheritdoc/>
        public override string ToString() => "MEDIUMINT";
    }

    /// <summary>Unsigned 24-bit integer.</summary>
    public sealed class UnsignedMediumInt : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly UnsignedMediumInt Instance = new();

        private UnsignedMediumInt() { }

        /// <inheritdoc/>
        public override string ToString() => "MEDIUMINT UNSIGNED";
    }

    /// <summary>Signed 64-bit integer.</summary>
    public sealed class BigInt : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly BigInt Instance = new();

        private BigInt() { }

        /// <inheritdoc/>
        public override string ToString() => "BIGINT";
    }

    /// <summary>Unsigned 64-bit integer.</summary>
    public sealed class UnsignedBigInt : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly UnsignedBigInt Instance = new();

        private UnsignedBigInt() { }

        /// <inheritdoc/>
        public override string ToString() => "BIGINT UNSIGNED";
    }

    #endregion

    #region Floating-Point Types

    /// <summary>Single-precision floating-point number.</summary>
    public sealed class Float : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Float Instance = new();

        private Float() { }

        /// <inheritdoc/>
        public override string ToString() => "FLOAT";
    }

    /// <summary>Double-precision floating-point number.</summary>
    public sealed class Double : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Double Instance = new();

        private Double() { }

        /// <inheritdoc/>
        public override string ToString() => "DOUBLE";
    }

    /// <summary>Fixed-precision decimal number with optional precision and scale.</summary>
    public sealed class Decimal(ulong? precision, ulong? scale) : DataType
    {
        /// <summary>Optional total number of digits.</summary>
        public ulong? Precision { get; } = precision;

        /// <summary>Optional number of digits after the decimal point.</summary>
        public ulong? Scale { get; } = scale;

        /// <inheritdoc/>
        public override string ToString() =>
            (Precision, Scale) switch
            {
                (ulong p, ulong s) => $"DECIMAL({p},{s})",
                (ulong p, null) => $"DECIMAL({p})",
                _ => "DECIMAL",
            };
    }

    #endregion

    #region Boolean Type

    /// <summary>Boolean (true/false) type.</summary>
    public sealed class Boolean : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Boolean Instance = new();

        private Boolean() { }

        /// <inheritdoc/>
        public override string ToString() => "BOOLEAN";
    }

    #endregion

    #region Date and Time Types

    /// <summary>Calendar date without time.</summary>
    public sealed class Date : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Date Instance = new();

        private Date() { }

        /// <inheritdoc/>
        public override string ToString() => "DATE";
    }

    /// <summary>Time of day without timezone.</summary>
    public sealed class Time : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Time Instance = new();

        private Time() { }

        /// <inheritdoc/>
        public override string ToString() => "TIME";
    }

    /// <summary>Time of day with timezone.</summary>
    public sealed class TimeTz : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly TimeTz Instance = new();

        private TimeTz() { }

        /// <inheritdoc/>
        public override string ToString() => "TIME WITH TIME ZONE";
    }

    /// <summary>Date and time without timezone.</summary>
    public sealed class DateTime : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly DateTime Instance = new();

        private DateTime() { }

        /// <inheritdoc/>
        public override string ToString() => "DATETIME";
    }

    /// <summary>Date and time without timezone (TIMESTAMP variant).</summary>
    public sealed class Timestamp : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Timestamp Instance = new();

        private Timestamp() { }

        /// <inheritdoc/>
        public override string ToString() => "TIMESTAMP";
    }

    /// <summary>Date and time with timezone.</summary>
    public sealed class TimestampTz : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly TimestampTz Instance = new();

        private TimestampTz() { }

        /// <inheritdoc/>
        public override string ToString() => "TIMESTAMP WITH TIME ZONE";
    }

    #endregion

    #region String and Text Types

    /// <summary>Fixed-length character string with optional length.</summary>
    public sealed class Char(ulong? length) : DataType
    {
        /// <summary>Optional maximum character length.</summary>
        public ulong? Length { get; } = length;

        /// <inheritdoc/>
        public override string ToString() => Length.HasValue ? $"CHAR({Length})" : "CHAR";
    }

    /// <summary>Variable-length character string with optional maximum length.</summary>
    public sealed class Varchar(ulong? length) : DataType
    {
        /// <summary>Optional maximum character length.</summary>
        public ulong? Length { get; } = length;

        /// <inheritdoc/>
        public override string ToString() => Length.HasValue ? $"VARCHAR({Length})" : "TEXT";
    }

    /// <summary>Small variable-length text.</summary>
    public sealed class TinyText : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly TinyText Instance = new();

        private TinyText() { }

        /// <inheritdoc/>
        public override string ToString() => "TINYTEXT";
    }

    /// <summary>Medium variable-length text.</summary>
    public sealed class MediumText : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly MediumText Instance = new();

        private MediumText() { }

        /// <inheritdoc/>
        public override string ToString() => "MEDIUMTEXT";
    }

    /// <summary>Large variable-length text.</summary>
    public sealed class LongText : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly LongText Instance = new();

        private LongText() { }

        /// <inheritdoc/>
        public override string ToString() => "LONGTEXT";
    }

    #endregion

    #region Binary Types

    /// <summary>Fixed-length binary string with optional length.</summary>
    public sealed class Binary(ulong? length) : DataType
    {
        /// <summary>Optional byte length.</summary>
        public ulong? Length { get; } = length;

        /// <inheritdoc/>
        public override string ToString() => Length.HasValue ? $"BINARY({Length})" : "BINARY";
    }

    /// <summary>Variable-length binary string with optional maximum length.</summary>
    public sealed class Varbinary(ulong? length) : DataType
    {
        /// <summary>Optional maximum byte length.</summary>
        public ulong? Length { get; } = length;

        /// <inheritdoc/>
        public override string ToString() => Length.HasValue ? $"VARBINARY({Length})" : "VARBINARY";
    }

    /// <summary>Binary large object with optional maximum size.</summary>
    public sealed class Blob(ulong? length) : DataType
    {
        /// <summary>Optional maximum byte length.</summary>
        public ulong? Length { get; } = length;

        /// <inheritdoc/>
        public override string ToString() => Length.HasValue ? $"BLOB({Length})" : "BLOB";
    }

    /// <summary>Small binary large object.</summary>
    public sealed class TinyBlob : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly TinyBlob Instance = new();

        private TinyBlob() { }

        /// <inheritdoc/>
        public override string ToString() => "TINYBLOB";
    }

    /// <summary>Medium binary large object.</summary>
    public sealed class MediumBlob : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly MediumBlob Instance = new();

        private MediumBlob() { }

        /// <inheritdoc/>
        public override string ToString() => "MEDIUMBLOB";
    }

    /// <summary>Large binary large object.</summary>
    public sealed class LongBlob : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly LongBlob Instance = new();

        private LongBlob() { }

        /// <inheritdoc/>
        public override string ToString() => "LONGBLOB";
    }

    #endregion

    #region Special and Reference Types

    /// <summary>Universally unique identifier.</summary>
    public sealed class Uuid : DataType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Uuid Instance = new();

        private Uuid() { }

        /// <inheritdoc/>
        public override string ToString() => "UUID";
    }

    /// <summary>A typed vector (array) of a homogeneous element type.</summary>
    public sealed class Vector(DataType elementType) : DataType
    {
        /// <summary>The data type of each element in the vector.</summary>
        public DataType ElementType { get; } = elementType;

        /// <inheritdoc/>
        public override string ToString() => $"[{ElementType}]";
    }

    /// <summary>A reference to a row in another table by table name.</summary>
    public sealed class Reference(string table) : DataType
    {
        /// <summary>The target table name.</summary>
        public string Table { get; } = table;

        /// <inheritdoc/>
        public override string ToString() => Table;
    }

    /// <summary>An array of references to rows in another table.</summary>
    public sealed class ReferenceArray(string table) : DataType
    {
        /// <summary>The target table name.</summary>
        public string Table { get; } = table;

        /// <inheritdoc/>
        public override string ToString() => $"[{Table}]";
    }

    /// <summary>A virtual (computed) reference to a column in another table.</summary>
    public sealed class VirtualReference(string table, string column) : DataType
    {
        /// <summary>The target table name.</summary>
        public string Table { get; } = table;

        /// <summary>The target column name.</summary>
        public string Column { get; } = column;

        /// <inheritdoc/>
        public override string ToString() => $"~{Table}({Column})";
    }

    /// <summary>An array of virtual (computed) references to a column in another table.</summary>
    public sealed class VirtualReferenceArray(string table, string column) : DataType
    {
        /// <summary>The target table name.</summary>
        public string Table { get; } = table;

        /// <summary>The target column name.</summary>
        public string Column { get; } = column;

        /// <inheritdoc/>
        public override string ToString() => $"~[{Table}]({Column})";
    }

    /// <summary>A reference to a user-defined enum type by name.</summary>
    public sealed class EnumRef(string name) : DataType
    {
        /// <summary>The registered enum type name.</summary>
        public string Name { get; } = name;

        /// <inheritdoc/>
        public override string ToString() => Name;
    }

    /// <summary>A data type not covered by the built-in variants.</summary>
    public sealed class Other(string name) : DataType
    {
        /// <summary>The raw type name as it appeared in SQL.</summary>
        public string Name { get; } = name;

        /// <inheritdoc/>
        public override string ToString() => Name;
    }

    #endregion
}
