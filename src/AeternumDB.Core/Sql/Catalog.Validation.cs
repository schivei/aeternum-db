using AeternumDB.Core.Errors;

namespace AeternumDB.Core.Sql;

/// <summary>Catalog with schema metadata, DDL/index mutations, and optional persistence for semantic validation.</summary>
public sealed partial class Catalog
{
    private readonly Dictionary<string, TableSchema> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, UserTypeSchema> _types = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Register a table in the catalog.</summary>
    public void AddTable(TableSchema schema)
    {
        lock (SyncRoot)
        {
            _tables[schema.Name.ToLowerInvariant()] = schema;
            PersistIfConfiguredUnsafe();
        }
    }

    /// <summary>Remove a table from the catalog.</summary>
    public void RemoveTable(string name)
    {
        lock (SyncRoot)
        {
            var removed = _tables.Remove(name.ToLowerInvariant());
            if (!removed)
                return;

            RemoveIndexesForTableUnsafe(name);
            PersistIfConfiguredUnsafe();
        }
    }

    /// <summary>Check whether a table exists.</summary>
    public bool TableExists(string name)
    {
        lock (SyncRoot)
            return _tables.ContainsKey(name.ToLowerInvariant());
    }

    /// <summary>Retrieve a table schema.</summary>
    public TableSchema? GetTable(string name)
    {
        lock (SyncRoot)
        {
            _tables.TryGetValue(name.ToLowerInvariant(), out var schema);
            return schema;
        }
    }

    /// <summary>Register a user-defined type.</summary>
    public void AddType(UserTypeSchema schema)
    {
        lock (SyncRoot)
        {
            _types[schema.Name.ToLowerInvariant()] = schema;
            PersistIfConfiguredUnsafe();
        }
    }

    /// <summary>Retrieve a user-defined type by name (case-insensitive).</summary>
    public UserTypeSchema? GetType(string name)
    {
        lock (SyncRoot)
        {
            _types.TryGetValue(name.ToLowerInvariant(), out var schema);
            return schema;
        }
    }

    /// <summary>Check whether a user-defined type exists.</summary>
    public bool TypeExists(string name)
    {
        lock (SyncRoot)
            return _types.ContainsKey(name.ToLowerInvariant());
    }

    /// <summary>
    /// Remove a user-defined type. Throws <see cref="PlannerException"/> if the type is still in use.
    /// </summary>
    public void RemoveType(string name)
    {
        lock (SyncRoot)
        {
            if (IsTypeInUseUnsafe(name))
                throw new PlannerException(PlannerErrorKind.CatalogError,
                    $"cannot drop type '{name}': it is still referenced by one or more columns");
            _types.Remove(name.ToLowerInvariant());
            PersistIfConfiguredUnsafe();
        }
    }

    /// <summary>Returns true if any column in any table references this type via EnumRef.</summary>
    public bool IsTypeInUse(string name)
    {
        lock (SyncRoot)
            return IsTypeInUseUnsafe(name);
    }

    private bool IsTypeInUseUnsafe(string name)
    {
        return _tables.Values
            .SelectMany(table => table.Columns)
            .Where(col => col.UserDefinedTypeName is not null)
            .Any(col => string.Equals(col.UserDefinedTypeName, name, StringComparison.OrdinalIgnoreCase));
    }
}
