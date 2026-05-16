using System.Text.Json.Serialization;
using AeternumDB.Core.Sql.CatalogSnapshots;

namespace AeternumDB.Core.Sql.CatalogPersistence;

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(PersistedDataTypeSnapshot))]
internal sealed partial class CatalogPersistedDataTypeJsonContext : JsonSerializerContext
{
}
