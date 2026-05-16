using System.Text.Json;
using AeternumDB.Core.Errors;

namespace AeternumDB.Core.Sql;

/// <summary>Metadata for a registered index.</summary>
public sealed class IndexSchema
{
    public string Name { get; }
    public string Table { get; }
    public IReadOnlyList<string> Columns { get; }
    public bool Unique { get; }
    public IndexType IndexType { get; }
    public DateTimeOffset CreatedAt { get; }

    public IndexSchema(
        string name,
        string table,
        IReadOnlyList<string> columns,
        bool unique,
        IndexType indexType,
        DateTimeOffset? createdAt = null)
    {
        Name = name;
        Table = table;
        Columns = columns;
        Unique = unique;
        IndexType = indexType;
        CreatedAt = createdAt ?? DateTimeOffset.UtcNow;
    }
}

public sealed partial class Catalog
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    private readonly object _syncRoot = new();
    private readonly Dictionary<string, IndexSchema> _indexes = new(StringComparer.OrdinalIgnoreCase);
    private readonly string? _persistencePath;
    private long _nextObjectId = 1;

    public Catalog(string? persistencePath = null)
    {
        _persistencePath = string.IsNullOrWhiteSpace(persistencePath) ? null : persistencePath;
        if (!string.IsNullOrWhiteSpace(_persistencePath))
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

            var tableName = schema.Name;
            var columns = schema.Columns.ToList();

            foreach (var op in stmt.Operations)
            {
                switch (op)
                {
                    case AlterTableOperation.AddColumn add:
                        if (columns.Any(c => string.Equals(c.Name, add.Column.Name, StringComparison.OrdinalIgnoreCase)))
                            throw new PlannerException(PlannerErrorKind.CatalogError, $"column '{add.Column.Name}' already exists");
                        columns.Add(new ColumnSchema(add.Column.Name, add.Column.DataType, add.Column.Nullable));
                        break;

                    case AlterTableOperation.DropColumn drop:
                    {
                        var found = columns.RemoveAll(c => string.Equals(c.Name, drop.Name, StringComparison.OrdinalIgnoreCase)) > 0;
                        if (!found && !drop.IfExists)
                            throw new PlannerException(PlannerErrorKind.CatalogError, $"column '{drop.Name}' does not exist");
                        break;
                    }

                    case AlterTableOperation.RenameColumn rename:
                    {
                        var idx = columns.FindIndex(c => string.Equals(c.Name, rename.OldName, StringComparison.OrdinalIgnoreCase));
                        if (idx < 0)
                            throw new PlannerException(PlannerErrorKind.CatalogError, $"column '{rename.OldName}' does not exist");
                        if (columns.Any(c => string.Equals(c.Name, rename.NewName, StringComparison.OrdinalIgnoreCase)))
                            throw new PlannerException(PlannerErrorKind.CatalogError, $"column '{rename.NewName}' already exists");
                        var existing = columns[idx];
                        columns[idx] = new ColumnSchema(rename.NewName, existing.DataType, existing.Nullable);
                        RenameColumnInIndexesUnsafe(tableName, rename.OldName, rename.NewName);
                        break;
                    }

                    case AlterTableOperation.RenameTable rename:
                    {
                        var newKey = rename.NewName.ToLowerInvariant();
                        if (_tables.ContainsKey(newKey))
                            throw new PlannerException(PlannerErrorKind.CatalogError, $"table '{rename.NewName}' already exists");
                        tableName = rename.NewName;
                        break;
                    }
                }
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
            RenameTableInIndexesUnsafe(schema.Name, tableName);
            PersistIfConfiguredUnsafe();
        }
    }

    public bool CreateIndex(CreateIndexStatement stmt)
    {
        lock (_syncRoot)
        {
            var table = GetTable(stmt.Table)
                ?? throw new PlannerException(PlannerErrorKind.CatalogError, $"table '{stmt.Table}' does not exist");
            foreach (var col in stmt.Columns)
            {
                if (table.GetColumn(col.Name) is null)
                    throw new PlannerException(PlannerErrorKind.CatalogError, $"column '{col.Name}' does not exist in table '{stmt.Table}'");
            }

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
        if (string.IsNullOrWhiteSpace(_persistencePath))
            return;

        var state = SnapshotStateUnsafe();
        var json = JsonSerializer.Serialize(state, JsonOptions);

        var dir = Path.GetDirectoryName(_persistencePath);
        if (!string.IsNullOrWhiteSpace(dir))
            Directory.CreateDirectory(dir);

        var tmp = $"{_persistencePath}.tmp";
        File.WriteAllText(tmp, json);
        File.Move(tmp, _persistencePath!, overwrite: true);
    }

    private void LoadFromPersistenceUnsafe()
    {
        if (string.IsNullOrWhiteSpace(_persistencePath))
            return;

        var primary = _persistencePath!;
        var tmp = $"{primary}.tmp";
        var source = File.Exists(primary)
            ? primary
            : (File.Exists(tmp) ? tmp : null);

        if (source is null)
            return;

        var json = File.ReadAllText(source);
        var state = JsonSerializer.Deserialize<CatalogStateSnapshot>(json, JsonOptions)
            ?? new CatalogStateSnapshot();

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
                "enum" => new UserTypeKind.Enum(
                    t.Flag,
                    t.Variants.Select(v => new EnumVariant(v.Name, v.IsNone)).ToList(),
                    t.ResolvedValues),
                "composite" => new UserTypeKind.Composite(
                    t.Fields.Select(f => (f.Name, ParseDataType(f.DataTypeText, null))).ToList()),
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
                    DataTypeText = c.DataType.ToString(),
                    Nullable = c.Nullable,
                    UserDefinedTypeName = c.UserDefinedTypeName
                }).ToList()
            }).ToList(),
            Types = _types.Values.Select(t =>
            {
                if (t.Kind is UserTypeKind.Enum e)
                {
                    return new TypeSnapshot
                    {
                        Name = t.Name,
                        Kind = "enum",
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
                        Kind = "composite",
                        Fields = c.Fields.Select(f => new TypeFieldSnapshot
                        {
                            Name = f.Name,
                            DataTypeText = f.Type.ToString()
                        }).ToList()
                    };
                }

                return new TypeSnapshot { Name = t.Name, Kind = "composite" };
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

    private static DataType ParseDataType(string text, string? userDefinedTypeName)
    {
        if (!string.IsNullOrWhiteSpace(userDefinedTypeName))
            return new DataType.EnumRef(userDefinedTypeName);

        return text.ToUpperInvariant() switch
        {
            "BOOLEAN" => DataType.Boolean.Instance,
            "INTEGER" => DataType.Integer.Instance,
            "BIGINT" => DataType.BigInt.Instance,
            "TIMESTAMP" => DataType.Timestamp.Instance,
            _ => new DataType.Other(text)
        };
    }

    private static string IndexTypeName(IndexType type) =>
        type switch
        {
            IndexType.BTree => "BTREE",
            IndexType.Hash => "HASH",
            IndexType.Gin => "GIN",
            IndexType.Gist => "GIST",
            IndexType.SpGist => "SPGIST",
            IndexType.Brin => "BRIN",
            IndexType.Bloom => "BLOOM",
            IndexType.FullText => "FULLTEXT",
            IndexType.Trigram => "TRIGRAM",
            IndexType.Other o => o.Name,
            _ => "BTREE"
        };

    private static IndexType ParseIndexType(string name) =>
        name.ToUpperInvariant() switch
        {
            "BTREE" => IndexType.BTree.Instance,
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

    private sealed class CatalogStateSnapshot
    {
        public List<TableSnapshot> Tables { get; set; } = [];
        public List<TypeSnapshot> Types { get; set; } = [];
        public List<IndexSnapshot> Indexes { get; set; } = [];
        public long NextObjectId { get; set; } = 1;
    }

    private sealed class TableSnapshot
    {
        public string Name { get; set; } = "";
        public int SchemaVersion { get; set; } = 1;
        public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
        public DateTimeOffset ModifiedAt { get; set; } = DateTimeOffset.UtcNow;
        public long RowCount { get; set; }
        public List<ColumnSnapshot> Columns { get; set; } = [];
    }

    private sealed class ColumnSnapshot
    {
        public string Name { get; set; } = "";
        public string DataTypeText { get; set; } = "UNKNOWN";
        public bool Nullable { get; set; } = true;
        public string? UserDefinedTypeName { get; set; }
    }

    private sealed class TypeSnapshot
    {
        public string Name { get; set; } = "";
        public string Kind { get; set; } = "composite";
        public bool Flag { get; set; }
        public List<EnumVariantSnapshot> Variants { get; set; } = [];
        public List<ulong> ResolvedValues { get; set; } = [];
        public List<TypeFieldSnapshot> Fields { get; set; } = [];
    }

    private sealed class EnumVariantSnapshot
    {
        public string Name { get; set; } = "";
        public bool IsNone { get; set; }
    }

    private sealed class TypeFieldSnapshot
    {
        public string Name { get; set; } = "";
        public string DataTypeText { get; set; } = "UNKNOWN";
    }

    private sealed class IndexSnapshot
    {
        public string Name { get; set; } = "";
        public string Table { get; set; } = "";
        public List<string> Columns { get; set; } = [];
        public bool Unique { get; set; }
        public string IndexType { get; set; } = "BTREE";
        public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    }
}
