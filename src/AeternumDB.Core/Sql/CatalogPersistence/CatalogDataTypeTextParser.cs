using AeternumDB.Core.Sql;
using AeternumDB.Core.Sql.Ast;

namespace AeternumDB.Core.Sql.CatalogPersistence;

internal static class CatalogDataTypeTextParser
{
    public static DataType Parse(string text)
    {
        if (TryParseKnownDataTypeText(text, out var known))
            return known;

        if (TryParseVirtualReferenceArrayText(text, out var virtualRefArray))
            return virtualRefArray;
        if (TryParseVirtualReferenceText(text, out var virtualRef))
            return virtualRef;

        if (text.StartsWith('[') && text.EndsWith(']') && text.Length > 2)
        {
            var inner = text[1..^1];
            if (TryParseKnownDataTypeText(inner, out var element))
                return new DataType.Vector(element);
            return new DataType.ReferenceArray(inner);
        }

        return new DataType.Other(text);
    }

    private static bool TryParseKnownDataTypeText(string text, out DataType type)
    {
        var normalized = text.Trim();
        var upper = normalized.ToUpperInvariant();
        switch (upper)
        {
            case "BOOLEAN":
                type = DataType.Boolean.Instance;
                return true;
            case "INTEGER":
                type = DataType.Integer.Instance;
                return true;
            case "INTEGER UNSIGNED":
                type = DataType.UnsignedInt.Instance;
                return true;
            case "FLOAT":
                type = DataType.Float.Instance;
                return true;
            case "DOUBLE":
                type = DataType.Double.Instance;
                return true;
            case "TEXT":
                type = new DataType.Varchar(null);
                return true;
            case "DATE":
                type = DataType.Date.Instance;
                return true;
            case "TIMESTAMP":
                type = DataType.Timestamp.Instance;
                return true;
            case "DECIMAL":
                type = new DataType.Decimal(null, null);
                return true;
            case "TINYINT":
                type = DataType.TinyInt.Instance;
                return true;
            case "TINYINT UNSIGNED":
                type = DataType.UnsignedTinyInt.Instance;
                return true;
            case "SMALLINT":
                type = DataType.SmallInt.Instance;
                return true;
            case "SMALLINT UNSIGNED":
                type = DataType.UnsignedSmallInt.Instance;
                return true;
            case "MEDIUMINT":
                type = DataType.MediumInt.Instance;
                return true;
            case "MEDIUMINT UNSIGNED":
                type = DataType.UnsignedMediumInt.Instance;
                return true;
            case "BIGINT":
                type = DataType.BigInt.Instance;
                return true;
            case "BIGINT UNSIGNED":
                type = DataType.UnsignedBigInt.Instance;
                return true;
            case "CHAR":
                type = new DataType.Char(null);
                return true;
            case "TINYTEXT":
                type = DataType.TinyText.Instance;
                return true;
            case "MEDIUMTEXT":
                type = DataType.MediumText.Instance;
                return true;
            case "LONGTEXT":
                type = DataType.LongText.Instance;
                return true;
            case "TIME":
                type = DataType.Time.Instance;
                return true;
            case "TIME WITH TIME ZONE":
                type = DataType.TimeTz.Instance;
                return true;
            case "DATETIME":
                type = DataType.DateTime.Instance;
                return true;
            case "TIMESTAMP WITH TIME ZONE":
                type = DataType.TimestampTz.Instance;
                return true;
            case "UUID":
                type = DataType.Uuid.Instance;
                return true;
            case "BINARY":
                type = new DataType.Binary(null);
                return true;
            case "VARBINARY":
                type = new DataType.Varbinary(null);
                return true;
            case "BLOB":
                type = new DataType.Blob(null);
                return true;
            case "TINYBLOB":
                type = DataType.TinyBlob.Instance;
                return true;
            case "MEDIUMBLOB":
                type = DataType.MediumBlob.Instance;
                return true;
            case "LONGBLOB":
                type = DataType.LongBlob.Instance;
                return true;
        }

        if (TryParseSingleUnsignedParameter(upper, "VARCHAR(", out var varcharLength))
        {
            type = new DataType.Varchar(varcharLength);
            return true;
        }
        if (TryParseSingleUnsignedParameter(upper, "CHAR(", out var charLength))
        {
            type = new DataType.Char(charLength);
            return true;
        }
        if (TryParseSingleUnsignedParameter(upper, "BINARY(", out var binaryLength))
        {
            type = new DataType.Binary(binaryLength);
            return true;
        }
        if (TryParseSingleUnsignedParameter(upper, "VARBINARY(", out var varbinaryLength))
        {
            type = new DataType.Varbinary(varbinaryLength);
            return true;
        }
        if (TryParseSingleUnsignedParameter(upper, "BLOB(", out var blobLength))
        {
            type = new DataType.Blob(blobLength);
            return true;
        }
        if (TryParseDecimal(upper, out var precision, out var scale))
        {
            type = new DataType.Decimal(precision, scale);
            return true;
        }

        type = default!;
        return false;
    }

    private static bool TryParseSingleUnsignedParameter(
        string upperText,
        string prefix,
        out ulong? value
    )
    {
        value = null;
        if (!upperText.StartsWith(prefix, StringComparison.Ordinal) || !upperText.EndsWith(')'))
            return false;

        var inside = upperText[prefix.Length..^1].Trim();
        if (!ulong.TryParse(inside, out var parsed))
            return false;
        if (parsed > CatalogDataTypePersistenceConstants.MaxTypeParameterValue)
            return false;

        value = parsed;
        return true;
    }

    private static bool TryParseDecimal(string upperText, out ulong? precision, out ulong? scale)
    {
        precision = null;
        scale = null;
        if (!upperText.StartsWith("DECIMAL(", StringComparison.Ordinal) || !upperText.EndsWith(')'))
            return false;

        var inside = upperText["DECIMAL(".Length..^1].Trim();
        if (inside.Length == 0)
            return true;

        var parts = inside.Split(
            ',',
            StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries
        );
        if (parts.Length is < 1 or > 2)
            return false;
        if (!ulong.TryParse(parts[0], out var parsedPrecision))
            return false;
        if (parsedPrecision > CatalogDataTypePersistenceConstants.MaxTypeParameterValue)
            return false;

        precision = parsedPrecision;
        if (parts.Length == 2)
        {
            if (!ulong.TryParse(parts[1], out var parsedScale))
                return false;
            if (parsedScale > CatalogDataTypePersistenceConstants.MaxTypeParameterValue)
                return false;
            scale = parsedScale;
        }

        return true;
    }

    private static bool TryParseVirtualReferenceText(string text, out DataType type)
    {
        type = default!;
        if (!text.StartsWith('~') || text.StartsWith("~[", StringComparison.Ordinal))
            return false;

        var open = text.IndexOf('(');
        if (open <= 1 || !text.EndsWith(')'))
            return false;

        var table = text[1..open];
        var column = text[(open + 1)..^1];
        if (table.Length == 0 || column.Length == 0)
            return false;

        type = new DataType.VirtualReference(table, column);
        return true;
    }

    private static bool TryParseVirtualReferenceArrayText(string text, out DataType type)
    {
        type = default!;
        if (!text.StartsWith("~[", StringComparison.Ordinal))
            return false;

        var close = text.IndexOf(']');
        var open = text.IndexOf('(');
        if (close < 2 || open < close + 1 || !text.EndsWith(')'))
            return false;

        var table = text[2..close];
        var column = text[(open + 1)..^1];
        if (table.Length == 0 || column.Length == 0)
            return false;

        type = new DataType.VirtualReferenceArray(table, column);
        return true;
    }
}
