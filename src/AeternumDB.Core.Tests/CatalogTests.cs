using AeternumDB.Core.Errors;
using AeternumDB.Core.Sql;

namespace AeternumDB.Core.Tests;

public sealed class CatalogTests
{
    [Fact]
    public void TableLifecycle_AlterAndIndexRegistry_Works()
    {
        var catalog = new Catalog();

        var created = catalog.CreateTable(new CreateTableStatement
        {
            Table = "users",
            Columns =
            [
                new ColumnDef { Name = "id", DataType = DataType.BigInt.Instance, Nullable = false },
                new ColumnDef { Name = "name", DataType = new DataType.Varchar(255), Nullable = false }
            ]
        });

        Assert.True(created);
        var schemaV1 = catalog.GetTable("users");
        Assert.NotNull(schemaV1);
        Assert.Equal(1, schemaV1!.SchemaVersion);

        catalog.AlterTable(new AlterTableStatement(
            "users",
            [
                new AlterTableOperation.AddColumn(new ColumnDef
                {
                    Name = "email",
                    DataType = new DataType.Varchar(255),
                    Nullable = true
                })
            ]));

        var schemaV2 = catalog.GetTable("users");
        Assert.NotNull(schemaV2);
        Assert.Equal(2, schemaV2!.SchemaVersion);
        Assert.NotNull(schemaV2.GetColumn("email"));

        var indexCreated = catalog.CreateIndex(new CreateIndexStatement
        {
            Name = "users_email_idx",
            Table = "users",
            Columns = [new IndexColumn("email", true)],
            Unique = false,
            IndexType = IndexType.BTree.Instance
        });

        Assert.True(indexCreated);
        Assert.Single(catalog.GetTableIndexes("users"));

        catalog.DropTable("users");
        Assert.False(catalog.TableExists("users"));
        Assert.Empty(catalog.GetTableIndexes("users"));
    }

    [Fact]
    public void AlterTable_RenameTableThenRenameColumn_UpdatesIndexMetadata()
    {
        var catalog = new Catalog();
        catalog.CreateTable(new TableSchema(
            "users",
            [
                new ColumnSchema("id", DataType.BigInt.Instance, nullable: false),
                new ColumnSchema("email", new DataType.Varchar(255), nullable: false)
            ]));

        catalog.CreateIndex(new CreateIndexStatement
        {
            Name = "users_email_idx",
            Table = "users",
            Columns = [new IndexColumn("email", true)],
            Unique = false,
            IndexType = IndexType.BTree.Instance
        });

        catalog.AlterTable(new AlterTableStatement(
            "users",
            [
                new AlterTableOperation.RenameTable("users_v2"),
                new AlterTableOperation.RenameColumn("email", "contact_email")
            ]));

        var indexes = catalog.GetTableIndexes("users_v2");
        var index = Assert.Single(indexes);
        Assert.Equal(["contact_email"], index.Columns);
    }

    [Fact]
    public void RemoveType_WhenReferencedByTable_Throws()
    {
        var catalog = new Catalog();
        catalog.AddType(new UserTypeSchema(
            "status",
            new UserTypeKind.Enum(false, [new EnumVariant("OPEN"), new EnumVariant("CLOSED")], [1, 2])));

        catalog.CreateTable(new TableSchema(
            "tickets",
            [new ColumnSchema("state", new DataType.EnumRef("status"), nullable: false)]));

        var ex = Assert.Throws<PlannerException>(() => catalog.RemoveType("status"));
        Assert.Equal(PlannerErrorKind.CatalogError, ex.Kind);
    }

    [Fact]
    public void CatalogPersistence_RoundtripRestoresTablesIndexesTypesAndObjId()
    {
        var fileName = $"aeternum-catalog-{Guid.NewGuid():N}.json";
        var path = Path.GetFullPath(fileName, Path.GetTempPath());
        try
        {
            var catalog = new Catalog(path);
            catalog.AddType(new UserTypeSchema(
                "priority",
                new UserTypeKind.Enum(false, [new EnumVariant("LOW"), new EnumVariant("HIGH")], [1, 2])));
            catalog.CreateTable(new TableSchema(
                "tasks",
                [
                    new ColumnSchema("id", DataType.BigInt.Instance, nullable: false),
                    new ColumnSchema("priority", new DataType.EnumRef("priority"), nullable: false),
                    new ColumnSchema("name", new DataType.Varchar(255), nullable: false),
                    new ColumnSchema("ratio", new DataType.Decimal(10, 2), nullable: true),
                    new ColumnSchema("assignee_ref", new DataType.Reference("users"), nullable: true),
                    new ColumnSchema("assignee_refs", new DataType.ReferenceArray("users"), nullable: true),
                    new ColumnSchema("tags", new DataType.Vector(new DataType.Varchar(50)), nullable: true)
                ]));
            catalog.AddIndex(new IndexSchema(
                "tasks_priority_idx",
                "tasks",
                ["priority"],
                unique: false,
                IndexType.BTree.Instance));

            _ = catalog.NextObjectId();
            var second = catalog.NextObjectId();

            var reloaded = new Catalog(path);
            Assert.True(reloaded.TableExists("tasks"));
            Assert.True(reloaded.TypeExists("priority"));
            Assert.True(reloaded.IndexExists("tasks_priority_idx"));
            Assert.Equal(second + 1, reloaded.NextObjectId());
            Assert.IsType<DataType.Varchar>(reloaded.GetTable("tasks")!.GetColumn("name")!.DataType);
            Assert.IsType<DataType.Decimal>(reloaded.GetTable("tasks")!.GetColumn("ratio")!.DataType);
            Assert.IsType<DataType.Reference>(reloaded.GetTable("tasks")!.GetColumn("assignee_ref")!.DataType);
            Assert.IsType<DataType.ReferenceArray>(reloaded.GetTable("tasks")!.GetColumn("assignee_refs")!.DataType);
            Assert.IsType<DataType.Vector>(reloaded.GetTable("tasks")!.GetColumn("tags")!.DataType);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
            if (File.Exists($"{path}.tmp"))
                File.Delete($"{path}.tmp");
        }
    }

    [Fact]
    public void CatalogConcurrentCreateTable_IsThreadSafe()
    {
        var catalog = new Catalog();

        Parallel.For(0, 256, i =>
        {
            var created = catalog.CreateTable(
                new TableSchema($"t_{i}", [new ColumnSchema("id", DataType.Integer.Instance, nullable: false)]),
                ifNotExists: true);
            Assert.True(created);
        });

        for (var i = 0; i < 256; i++)
            Assert.True(catalog.TableExists($"t_{i}"));
    }

    [Fact]
    public void CatalogPersistence_InvalidJson_ThrowsCatalogError()
    {
        var fileName = $"aeternum-catalog-invalid-{Guid.NewGuid():N}.json";
        var path = Path.GetFullPath(fileName, Path.GetTempPath());
        try
        {
            File.WriteAllText(path, "{ not-valid-json }");
            var ex = Assert.Throws<PlannerException>(() => _ = new Catalog(path));
            Assert.Equal(PlannerErrorKind.CatalogError, ex.Kind);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
            if (File.Exists($"{path}.tmp"))
                File.Delete($"{path}.tmp");
        }
    }

    [Fact]
    public void TableSchema_DefaultCreatedAndModifiedAt_AreEqual()
    {
        var schema = new TableSchema("t", [new ColumnSchema("id", DataType.BigInt.Instance, nullable: false)]);
        Assert.Equal(schema.CreatedAt, schema.ModifiedAt);
    }

    [Fact]
    public void RemoveTable_NonExisting_DoesNotRemoveUnrelatedIndexes()
    {
        var catalog = new Catalog();
        catalog.CreateTable(new TableSchema(
            "users",
            [new ColumnSchema("id", DataType.BigInt.Instance, nullable: false)]));
        catalog.CreateIndex(new CreateIndexStatement
        {
            Name = "users_id_idx",
            Table = "users",
            Columns = [new IndexColumn("id", true)],
            Unique = false,
            IndexType = IndexType.BTree.Instance
        });

        catalog.RemoveTable("orders");

        Assert.True(catalog.IndexExists("users_id_idx"));
    }
}
