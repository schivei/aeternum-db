using System.Text.Json;
using System.Text.Json.Serialization;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Sql.CatalogPersistence;
using AeternumDB.Core.Sql.CatalogSnapshots;

namespace AeternumDB.Core.Sql;

public sealed partial class Catalog
{
    private const string EnumKind = CatalogSnapshotConstants.EnumKind;
    private const string CompositeKind = CatalogSnapshotConstants.CompositeKind;
    private const string DefaultIndexTypeName = CatalogSnapshotConstants.DefaultIndexTypeName;

    private readonly object _syncRoot = new();
    private readonly Dictionary<string, IndexSchema> _indexes = new(StringComparer.OrdinalIgnoreCase);
    private readonly string? _persistencePath;
    private long _nextObjectId = 1;

    public Catalog(string? persistencePath = null)
    {
        _persistencePath = string.IsNullOrWhiteSpace(persistencePath) ? null : persistencePath;
        if (_persistencePath is not null)
        {
            lock (_syncRoot)
            {
                LoadFromPersistenceUnsafe();
            }
        }
    }

    internal object SyncRoot => _syncRoot;

    public long NextObjectId()
    {
        lock (_syncRoot)
        {
            var id = _nextObjectId++;
            PersistIfConfiguredUnsafe();
            return id;
        }
    }

    public bool CreateTable(TableSchema schema, bool ifNotExists = false)
    {
        lock (_syncRoot)
        {
            var key = schema.Name.ToLowerInvariant();
            if (_tables.ContainsKey(key))
            {
                if (ifNotExists) return false;
                throw new PlannerException(PlannerErrorKind.CatalogError, $"table '{schema.Name}' already exists");
            }

            _tables[key] = schema;
            PersistIfConfiguredUnsafe();
            return true;
        }
    }

    public bool CreateTable(CreateTableStatement stmt)
    {
        var columns = stmt.Columns
            .Select(c => new ColumnSchema(c.Name, c.DataType, c.Nullable))
            .ToList();
        var schema = new TableSchema(stmt.Table, columns);
        return CreateTable(schema, stmt.IfNotExists);
    }

    public bool DropTable(string name, bool ifExists = false)
    {
        lock (_syncRoot)
        {
            var removed = _tables.Remove(name.ToLowerInvariant());
            if (!removed && !ifExists)
                throw new PlannerException(PlannerErrorKind.CatalogError, $"table '{name}' does not exist");

            if (removed)
            {
                RemoveIndexesForTableUnsafe(name);
                PersistIfConfiguredUnsafe();
            }

            return removed;
        }
    }

    public void DropTable(DropTableStatement stmt)
    {
        foreach (var table in stmt.Tables)
            DropTable(table, stmt.IfExists);
    }

    public void AlterTable(AlterTableStatement stmt)
    {
        lock (_syncRoot)
        {
            var key = stmt.Table.ToLowerInvariant();
            if (!_tables.TryGetValue(key, out var schema))
                throw new PlannerException(PlannerErrorKind.CatalogError, $"table '{stmt.Table}' does not exist");

            var originalTableName = schema.Name;
            var tableName = schema.Name;
            var columns = schema.Columns.ToList();

            foreach (var op in stmt.Operations)
            {
                ApplyAlterOperationUnsafe(op, originalTableName, ref tableName, columns);
            }

            var updated = new TableSchema(
                tableName,
                columns,
                schema.SchemaVersion + 1,
                schema.CreatedAt,
                DateTimeOffset.UtcNow,
                schema.RowCount);

            _tables.Remove(key);
            _tables[tableName.ToLowerInvariant()] = updated;
            RenameTableInIndexesUnsafe(originalTableName, tableName);
            PersistIfConfiguredUnsafe();
        }
    }

    public bool CreateIndex(CreateIndexStatement stmt)
    {
        lock (_syncRoot)
        {
            var table = GetTable(stmt.Table)
                ?? throw new PlannerException(PlannerErrorKind.CatalogError, $"table '{stmt.Table}' does not exist");

            var missingColumn = stmt.Columns
                .Where(col => table.GetColumn(col.Name) is null)
                .Select(col => col.Name)
                .FirstOrDefault();
            if (missingColumn is not null)
                throw new PlannerException(PlannerErrorKind.CatalogError, $"column '{missingColumn}' does not exist in table '{stmt.Table}'");

            var indexName = stmt.Name ?? $"{stmt.Table}_{string.Join("_", stmt.Columns.Select(c => c.Name))}_idx";
            var key = indexName.ToLowerInvariant();
            if (_indexes.ContainsKey(key))
            {
                if (stmt.IfNotExists) return false;
                throw new PlannerException(PlannerErrorKind.CatalogError, $"index '{indexName}' already exists");
            }

            _indexes[key] = new IndexSchema(
                indexName,
                table.Name,
                stmt.Columns.Select(c => c.Name).ToList(),
                stmt.Unique,
                stmt.IndexType);

            PersistIfConfiguredUnsafe();
            return true;
        }
    }

    public void AddIndex(IndexSchema schema)
    {
        lock (_syncRoot)
        {
            _indexes[schema.Name.ToLowerInvariant()] = schema;
            PersistIfConfiguredUnsafe();
        }
    }

    public bool IndexExists(string name)
    {
        lock (_syncRoot)
            return _indexes.ContainsKey(name.ToLowerInvariant());
    }

    public IReadOnlyList<IndexSchema> GetTableIndexes(string table)
    {
        lock (_syncRoot)
            return _indexes.Values
                .Where(i => string.Equals(i.Table, table, StringComparison.OrdinalIgnoreCase))
                .ToList();
    }

    public bool DropIndex(string name, bool ifExists = false)
    {
        lock (_syncRoot)
        {
            var removed = _indexes.Remove(name.ToLowerInvariant());
            if (!removed && !ifExists)
                throw new PlannerException(PlannerErrorKind.CatalogError, $"index '{name}' does not exist");

            if (removed)
                PersistIfConfiguredUnsafe();
            return removed;
        }
    }

    public void DropIndex(DropIndexStatement stmt)
    {
        foreach (var name in stmt.Names)
            DropIndex(name, stmt.IfExists);
    }

    public void Save()
    {
        lock (_syncRoot)
            PersistIfConfiguredUnsafe();
    }

    public void Load()
    {
        lock (_syncRoot)
            LoadFromPersistenceUnsafe();
    }

    internal void RemoveIndexesForTableUnsafe(string tableName)
    {
        var keys = _indexes
            .Where(x => string.Equals(x.Value.Table, tableName, StringComparison.OrdinalIgnoreCase))
            .Select(x => x.Key)
            .ToList();

        foreach (var key in keys)
            _indexes.Remove(key);
    }

    private void RenameTableInIndexesUnsafe(string oldTableName, string newTableName)
    {
        var replacements = _indexes
            .Where(x => string.Equals(x.Value.Table, oldTableName, StringComparison.OrdinalIgnoreCase))
            .Select(x => x.Key)
            .ToList();

        foreach (var key in replacements)
        {
            var idx = _indexes[key];
            _indexes[key] = new IndexSchema(
                idx.Name,
                newTableName,
                idx.Columns,
                idx.Unique,
                idx.IndexType,
                idx.CreatedAt);
        }
    }

    private void RenameColumnInIndexesUnsafe(string tableName, string oldColumn, string newColumn)
    {
        var replacements = _indexes
            .Where(x => string.Equals(x.Value.Table, tableName, StringComparison.OrdinalIgnoreCase)
                        && x.Value.Columns.Any(c => string.Equals(c, oldColumn, StringComparison.OrdinalIgnoreCase)))
            .Select(x => x.Key)
            .ToList();

        foreach (var key in replacements)
        {
            var idx = _indexes[key];
            var cols = idx.Columns
                .Select(c => string.Equals(c, oldColumn, StringComparison.OrdinalIgnoreCase) ? newColumn : c)
                .ToList();

            _indexes[key] = new IndexSchema(
                idx.Name,
                idx.Table,
                cols,
                idx.Unique,
                idx.IndexType,
                idx.CreatedAt);
        }
    }

    internal void PersistIfConfiguredUnsafe()
    {
        if (_persistencePath is null)
            return;

        var state = SnapshotStateUnsafe();
        var json = JsonSerializer.Serialize(state, CatalogJsonContext.Default.CatalogStateSnapshot);

        var dir = Path.GetDirectoryName(_persistencePath);
        if (!string.IsNullOrWhiteSpace(dir))
            Directory.CreateDirectory(dir);

        var tmp = $"{_persistencePath}.tmp";
        File.WriteAllText(tmp, json);
        File.Move(tmp, _persistencePath, overwrite: true);
    }

    private void LoadFromPersistenceUnsafe()
    {
        if (string.IsNullOrWhiteSpace(_persistencePath))
            return;

        var primary = _persistencePath!;
        var tmp = $"{primary}.tmp";
        string? source;
        if (File.Exists(primary))
            source = primary;
        else if (File.Exists(tmp))
            source = tmp;
        else
            source = null;

        if (source is null)
            return;

        CatalogStateSnapshot state;
        try
        {
            var json = File.ReadAllText(source);
            state = JsonSerializer.Deserialize(json, CatalogJsonContext.Default.CatalogStateSnapshot)
                ?? new CatalogStateSnapshot();
        }
        catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
        {
            throw new PlannerException(PlannerErrorKind.CatalogError, $"failed to load catalog metadata from '{source}': {ex.Message}");
        }

        _tables.Clear();
        _types.Clear();
        _indexes.Clear();

        foreach (var t in state.Tables)
        {
            var columns = t.Columns
                .Select(c => new ColumnSchema(c.Name, ParseDataType(c.DataTypeText, c.UserDefinedTypeName), c.Nullable))
                .ToList();
            _tables[t.Name.ToLowerInvariant()] = new TableSchema(
                t.Name,
                columns,
                t.SchemaVersion,
                t.CreatedAt,
                t.ModifiedAt,
                t.RowCount);
        }

        foreach (var t in state.Types)
        {
            UserTypeKind kind = t.Kind switch
            {
                EnumKind => new UserTypeKind.Enum(
                    t.Flag,
                    t.Variants.Select(v => new EnumVariant(v.Name, v.IsNone)).ToList(),
                    t.ResolvedValues),
                CompositeKind => new UserTypeKind.Composite(
                    t.Fields.Select(f => (f.Name, ParseDataType(f.DataTypeText, f.UserDefinedTypeName))).ToList()),
                _ => new UserTypeKind.Composite([])
            };

            _types[t.Name.ToLowerInvariant()] = new UserTypeSchema(t.Name, kind);
        }

        foreach (var idx in state.Indexes)
        {
            _indexes[idx.Name.ToLowerInvariant()] = new IndexSchema(
                idx.Name,
                idx.Table,
                idx.Columns,
                idx.Unique,
                ParseIndexType(idx.IndexType),
                idx.CreatedAt);
        }

        _nextObjectId = Math.Max(1, state.NextObjectId);
    }

    private CatalogStateSnapshot SnapshotStateUnsafe() =>
        new()
        {
            NextObjectId = _nextObjectId,
            Tables = _tables.Values.Select(t => new TableSnapshot
            {
                Name = t.Name,
                SchemaVersion = t.SchemaVersion,
                CreatedAt = t.CreatedAt,
                ModifiedAt = t.ModifiedAt,
                RowCount = t.RowCount,
                Columns = t.Columns.Select(c => new ColumnSnapshot
                {
                    Name = c.Name,
                    DataTypeText = SerializeDataType(c.DataType),
                    Nullable = c.Nullable,
                    UserDefinedTypeName = GetUserDefinedTypeName(c.DataType)
                }).ToList()
            }).ToList(),
            Types = _types.Values.Select(t =>
            {
                if (t.Kind is UserTypeKind.Enum e)
                {
                    return new TypeSnapshot
                    {
                        Name = t.Name,
                        Kind = EnumKind,
                        Flag = e.Flag,
                        Variants = e.Variants.Select(v => new EnumVariantSnapshot { Name = v.Name, IsNone = v.IsNone }).ToList(),
                        ResolvedValues = e.ResolvedValues.ToList()
                    };
                }

                if (t.Kind is UserTypeKind.Composite c)
                {
                    return new TypeSnapshot
                    {
                        Name = t.Name,
                        Kind = CompositeKind,
                        Fields = c.Fields.Select(f => new TypeFieldSnapshot
                        {
                            Name = f.Name,
                            DataTypeText = SerializeDataType(f.Type),
                            UserDefinedTypeName = GetUserDefinedTypeName(f.Type)
                        }).ToList()
                    };
                }

                return new TypeSnapshot { Name = t.Name, Kind = CompositeKind };
            }).ToList(),
            Indexes = _indexes.Values.Select(i => new IndexSnapshot
            {
                Name = i.Name,
                Table = i.Table,
                Columns = i.Columns.ToList(),
                Unique = i.Unique,
                IndexType = IndexTypeName(i.IndexType),
                CreatedAt = i.CreatedAt
            }).ToList()
        };

    private static DataType ParseDataType(string text, string? userDefinedTypeName) =>
        CatalogDataTypePersistenceConverter.ParseDataType(text, userDefinedTypeName);

    private static string SerializeDataType(DataType dataType) =>
        CatalogDataTypePersistenceConverter.SerializeDataType(dataType);

    private static string? GetUserDefinedTypeName(DataType type) =>
        CatalogDataTypePersistenceConverter.GetUserDefinedTypeName(type);

    private static string IndexTypeName(IndexType type) =>
        type switch
        {
            IndexType.BTree => DefaultIndexTypeName,
            IndexType.Hash => "HASH",
            IndexType.Gin => "GIN",
            IndexType.Gist => "GIST",
            IndexType.SpGist => "SPGIST",
            IndexType.Brin => "BRIN",
            IndexType.Bloom => "BLOOM",
            IndexType.FullText => "FULLTEXT",
            IndexType.Trigram => "TRIGRAM",
            IndexType.Other o => o.Name,
            _ => DefaultIndexTypeName
        };

    private static IndexType ParseIndexType(string name) =>
        name.ToUpperInvariant() switch
        {
            DefaultIndexTypeName => IndexType.BTree.Instance,
            "HASH" => IndexType.Hash.Instance,
            "GIN" => IndexType.Gin.Instance,
            "GIST" => IndexType.Gist.Instance,
            "SPGIST" => IndexType.SpGist.Instance,
            "BRIN" => IndexType.Brin.Instance,
            "BLOOM" => IndexType.Bloom.Instance,
            "FULLTEXT" => IndexType.FullText.Instance,
            "TRIGRAM" => IndexType.Trigram.Instance,
            _ => new IndexType.Other(name)
        };

    private void ApplyAlterOperationUnsafe(AlterTableOperation op, string indexTableName, ref string tableName, List<ColumnSchema> columns)
    {
        switch (op)
        {
            case AlterTableOperation.AddColumn add:
                AddColumnUnsafe(add, columns);
                break;
            case AlterTableOperation.DropColumn drop:
                DropColumnUnsafe(drop, columns);
                break;
            case AlterTableOperation.RenameColumn rename:
                RenameColumnUnsafe(rename, indexTableName, columns);
                break;
            case AlterTableOperation.RenameTable rename:
                tableName = RenameTableUnsafe(rename);
                break;
        }
    }

    private static void AddColumnUnsafe(AlterTableOperation.AddColumn add, List<ColumnSchema> columns)
    {
        if (columns.Any(c => string.Equals(c.Name, add.Column.Name, StringComparison.OrdinalIgnoreCase)))
            throw new PlannerException(PlannerErrorKind.CatalogError, $"column '{add.Column.Name}' already exists");
        columns.Add(new ColumnSchema(add.Column.Name, add.Column.DataType, add.Column.Nullable));
    }

    private static void DropColumnUnsafe(AlterTableOperation.DropColumn drop, List<ColumnSchema> columns)
    {
        var found = columns.RemoveAll(c => string.Equals(c.Name, drop.Name, StringComparison.OrdinalIgnoreCase)) > 0;
        if (!found && !drop.IfExists)
            throw new PlannerException(PlannerErrorKind.CatalogError, $"column '{drop.Name}' does not exist");
    }

    private void RenameColumnUnsafe(AlterTableOperation.RenameColumn rename, string tableName, List<ColumnSchema> columns)
    {
        var idx = columns.FindIndex(c => string.Equals(c.Name, rename.OldName, StringComparison.OrdinalIgnoreCase));
        if (idx < 0)
            throw new PlannerException(PlannerErrorKind.CatalogError, $"column '{rename.OldName}' does not exist");
        if (columns.Any(c => string.Equals(c.Name, rename.NewName, StringComparison.OrdinalIgnoreCase)))
            throw new PlannerException(PlannerErrorKind.CatalogError, $"column '{rename.NewName}' already exists");

        var existing = columns[idx];
        columns[idx] = new ColumnSchema(rename.NewName, existing.DataType, existing.Nullable);
        RenameColumnInIndexesUnsafe(tableName, rename.OldName, rename.NewName);
    }

    private string RenameTableUnsafe(AlterTableOperation.RenameTable rename)
    {
        var newKey = rename.NewName.ToLowerInvariant();
        if (_tables.ContainsKey(newKey))
            throw new PlannerException(PlannerErrorKind.CatalogError, $"table '{rename.NewName}' already exists");
        return rename.NewName;
    }

    [JsonSourceGenerationOptions(WriteIndented = true)]
    [JsonSerializable(typeof(CatalogStateSnapshot))]
    private sealed partial class CatalogJsonContext : JsonSerializerContext
    {
    }
}
