using AeternumDB.Core.Sql.CatalogSnapshots;

namespace AeternumDB.Core.Sql.CatalogPersistence;

internal static class CatalogDataTypePersistenceConstants
{
    public const string UnknownTypeName = CatalogSnapshotConstants.UnknownTypeName;
    public const ulong MaxTypeParameterValue = 1_000_000;
}
