using System.Text.Json;
using AeternumDB.Core.Sql;
using AeternumDB.Core.Sql.CatalogSnapshots;

namespace AeternumDB.Core.Sql.CatalogPersistence;

internal static class CatalogPersistedDataTypeJsonCodec
{
    public static string Serialize(PersistedDataTypeSnapshot payload) =>
        JsonSerializer.Serialize(payload, CatalogPersistedDataTypeJsonContext.Default.PersistedDataTypeSnapshot);

    public static bool TryParseDataType(string text, out DataType type)
    {
        type = default!;
        if (string.IsNullOrWhiteSpace(text) || text.TrimStart()[0] != '{')
            return false;

        try
        {
            var payload = JsonSerializer.Deserialize(text, CatalogPersistedDataTypeJsonContext.Default.PersistedDataTypeSnapshot);
            if (payload?.Kind is null)
                return false;

            type = payload.Kind switch
            {
                "array" => new DataType.Vector(CatalogDataTypePersistenceConverter.ParseDataType(payload.ElementTypeText ?? CatalogDataTypePersistenceConstants.UnknownTypeName, null)),
                "reference" => new DataType.Reference(payload.Table ?? payload.DataId ?? CatalogDataTypePersistenceConstants.UnknownTypeName),
                "reference_array" => new DataType.ReferenceArray(payload.Table ?? payload.DataId ?? CatalogDataTypePersistenceConstants.UnknownTypeName),
                "virtual_reference" => new DataType.VirtualReference(
                    payload.Table ?? CatalogDataTypePersistenceConstants.UnknownTypeName,
                    payload.Column ?? CatalogDataTypePersistenceConstants.UnknownTypeName),
                "virtual_reference_array" => new DataType.VirtualReferenceArray(
                    payload.Table ?? CatalogDataTypePersistenceConstants.UnknownTypeName,
                    payload.Column ?? CatalogDataTypePersistenceConstants.UnknownTypeName),
                "user_defined" => new DataType.EnumRef(payload.Name ?? payload.DataId ?? CatalogDataTypePersistenceConstants.UnknownTypeName),
                _ => default!
            };

            return type is not null;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
