using AeternumDB.Core.Sql;
using AeternumDB.Core.Sql.Ast;
using AeternumDB.Core.Sql.CatalogSnapshots;

namespace AeternumDB.Core.Sql.CatalogPersistence;

internal static class CatalogDataTypePersistenceConverter
{
    public static DataType ParseDataType(string text, string? userDefinedTypeName)
    {
        if (!string.IsNullOrWhiteSpace(userDefinedTypeName))
            return new DataType.EnumRef(userDefinedTypeName);

        if (CatalogPersistedDataTypeJsonCodec.TryParseDataType(text, out var jsonType))
            return jsonType;

        return CatalogDataTypeTextParser.Parse(text);
    }

    public static string SerializeDataType(DataType dataType)
    {
        PersistedDataTypeSnapshot? payload = dataType switch
        {
            DataType.Vector vector => new PersistedDataTypeSnapshot
            {
                Kind = "array",
                ElementTypeText = SerializeDataType(vector.ElementType),
            },
            DataType.Reference reference => new PersistedDataTypeSnapshot
            {
                Kind = "reference",
                Table = reference.Table,
                DataId = reference.Table.ToLowerInvariant(),
            },
            DataType.ReferenceArray referenceArray => new PersistedDataTypeSnapshot
            {
                Kind = "reference_array",
                Table = referenceArray.Table,
                DataId = referenceArray.Table.ToLowerInvariant(),
            },
            DataType.VirtualReference virtualReference => new PersistedDataTypeSnapshot
            {
                Kind = "virtual_reference",
                Table = virtualReference.Table,
                Column = virtualReference.Column,
                DataId =
                    $"{virtualReference.Table.ToLowerInvariant()}.{virtualReference.Column.ToLowerInvariant()}",
            },
            DataType.VirtualReferenceArray virtualReferenceArray => new PersistedDataTypeSnapshot
            {
                Kind = "virtual_reference_array",
                Table = virtualReferenceArray.Table,
                Column = virtualReferenceArray.Column,
                DataId =
                    $"{virtualReferenceArray.Table.ToLowerInvariant()}.{virtualReferenceArray.Column.ToLowerInvariant()}",
            },
            DataType.EnumRef enumRef => new PersistedDataTypeSnapshot
            {
                Kind = "user_defined",
                Name = enumRef.Name,
                DataId = enumRef.Name.ToLowerInvariant(),
            },
            _ => null,
        };

        if (payload is null)
            return dataType.ToString() ?? CatalogDataTypePersistenceConstants.UnknownTypeName;

        return CatalogPersistedDataTypeJsonCodec.Serialize(payload);
    }

    public static string? GetUserDefinedTypeName(DataType type) =>
        type is DataType.EnumRef enumRef ? enumRef.Name : null;
}
