using AeternumDB.Core.Errors;
using AeternumDB.Core.Sql;
using AeternumDB.Core.Sql.Ast;
using AeternumDB.Core.Sql.CatalogPersistence;

namespace AeternumDB.Core.Tests;

public sealed class SqlCoverageTests
{
    [Fact]
    public void Ast_SurfaceTypes_ConstructAndExposeExpectedValues()
    {
        var directive = new TermsDirective("status", TermsDirectiveKind.Text, ["OPEN", "CLOSED"]);
        Assert.Equal("status", directive.Name);
        Assert.Equal(TermsDirectiveKind.Text, directive.Kind);
        Assert.Equal(["OPEN", "CLOSED"], directive.EnumVariants);

        var textDirective = new TextDirective("en-US");
        Assert.Equal("en-US", textDirective.DefaultLocale);

        var enumDefinition = new TypeDefinition.Enum(
            flag: false,
            variants: [new EnumVariant("OPEN"), new EnumVariant("NONE", isNone: true)]
        );
        var compositeDefinition = new TypeDefinition.Composite(
            [new CompositeField("id", DataType.BigInt.Instance, notNull: true)]
        );
        Assert.Equal(2, enumDefinition.Variants.Count);
        Assert.Single(compositeDefinition.Fields);

        Assert.Equal("FALSE", new SqlValue.Boolean(false).ToString());
        Assert.Equal("3.5", new SqlValue.Float(3.5).ToString());
        Assert.Equal("'abc'", new SqlValue.SqlString("abc").ToString());
    }

    [Fact]
    public void CatalogDataTypeTextParser_Parse_CoversKnownAndFallbackPaths()
    {
        var text = Assert.IsType<DataType.Varchar>(CatalogDataTypeTextParser.Parse("TEXT"));
        Assert.Null(text.Length);

        var decimalType = Assert.IsType<DataType.Decimal>(CatalogDataTypeTextParser.Parse("DECIMAL(10,2)"));
        Assert.Equal((ulong)10, decimalType.Precision);
        Assert.Equal((ulong)2, decimalType.Scale);

        var decimalDefault = Assert.IsType<DataType.Decimal>(CatalogDataTypeTextParser.Parse("DECIMAL()"));
        Assert.Null(decimalDefault.Precision);
        Assert.Null(decimalDefault.Scale);

        var vector = Assert.IsType<DataType.Vector>(CatalogDataTypeTextParser.Parse("[INTEGER]"));
        Assert.IsType<DataType.Integer>(vector.ElementType);

        var referenceArray = Assert.IsType<DataType.ReferenceArray>(
            CatalogDataTypeTextParser.Parse("[external_table]")
        );
        Assert.Equal("external_table", referenceArray.Table);

        var virtualReference = Assert.IsType<DataType.VirtualReference>(
            CatalogDataTypeTextParser.Parse("~users(id)")
        );
        Assert.Equal("users", virtualReference.Table);
        Assert.Equal("id", virtualReference.Column);

        var virtualReferenceArray = Assert.IsType<DataType.VirtualReferenceArray>(
            CatalogDataTypeTextParser.Parse("~[users](id)")
        );
        Assert.Equal("users", virtualReferenceArray.Table);
        Assert.Equal("id", virtualReferenceArray.Column);

        var invalidDecimal = Assert.IsType<DataType.Other>(CatalogDataTypeTextParser.Parse("DECIMAL(1,2,3)"));
        Assert.Equal("DECIMAL(1,2,3)", invalidDecimal.Name);

        var tooLarge = Assert.IsType<DataType.Other>(CatalogDataTypeTextParser.Parse("VARCHAR(1000001)"));
        Assert.Equal("VARCHAR(1000001)", tooLarge.Name);
    }

    [Fact]
    public void CatalogPersistedDataTypeJsonCodec_ParseAndSerialize_CoversKindsAndFailures()
    {
        Assert.False(CatalogPersistedDataTypeJsonCodec.TryParseDataType("", out _));
        Assert.False(CatalogPersistedDataTypeJsonCodec.TryParseDataType("VARCHAR(100)", out _));
        Assert.False(CatalogPersistedDataTypeJsonCodec.TryParseDataType("{", out _));
        Assert.False(CatalogPersistedDataTypeJsonCodec.TryParseDataType("""{"name":"x"}""", out _));

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"array","ElementTypeText":"INTEGER"}""",
                out var arrayType
            )
        );
        Assert.IsType<DataType.Vector>(arrayType);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType("""{"Kind":"reference","Table":"users"}""", out var referenceType)
        );
        Assert.IsType<DataType.Reference>(referenceType);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"reference_array","Table":"users"}""",
                out var referenceArrayType
            )
        );
        Assert.IsType<DataType.ReferenceArray>(referenceArrayType);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"virtual_reference","Table":"profiles","Column":"id"}""",
                out var virtualReferenceType
            )
        );
        Assert.IsType<DataType.VirtualReference>(virtualReferenceType);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"virtual_reference_array","Table":"profiles","Column":"id"}""",
                out var virtualReferenceArrayType
            )
        );
        Assert.IsType<DataType.VirtualReferenceArray>(virtualReferenceArrayType);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType("""{"Kind":"user_defined","Name":"status"}""", out var enumRefType)
        );
        Assert.IsType<DataType.EnumRef>(enumRefType);

        Assert.False(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType("""{"Kind":"unknown"}""", out _)
        );
    }

    [Fact]
    public void CatalogDataTypePersistenceConverter_CoversDirectBranches()
    {
        var parsedUserDefined = CatalogDataTypePersistenceConverter.ParseDataType("INTEGER", "status");
        var parsedBuiltIn = CatalogDataTypePersistenceConverter.ParseDataType("INTEGER", null);
        Assert.IsType<DataType.EnumRef>(parsedUserDefined);
        Assert.IsType<DataType.Integer>(parsedBuiltIn);

        var serializedBuiltIn = CatalogDataTypePersistenceConverter.SerializeDataType(DataType.Integer.Instance);
        Assert.Equal("INTEGER", serializedBuiltIn);

        var serializedReference = CatalogDataTypePersistenceConverter.SerializeDataType(
            new DataType.Reference("users")
        );
        Assert.Contains("\"Kind\": \"reference\"", serializedReference);

        Assert.Equal(
            "status",
            CatalogDataTypePersistenceConverter.GetUserDefinedTypeName(new DataType.EnumRef("status"))
        );
        Assert.Null(CatalogDataTypePersistenceConverter.GetUserDefinedTypeName(DataType.Integer.Instance));
    }

    [Fact]
    public void SqlValidator_ValidateSequence_CoversTransactionBranches()
    {
        var validator = new SqlValidator(new Catalog());

        Assert.Throws<ValidationException.NoActiveTransactionException>(
            () =>
                validator.ValidateSequence(
                    [new Statement.Savepoint(new SavepointStatement("sp1"))]
                )
        );

        Assert.Throws<ValidationException.TransactionNameConflictException>(
            () =>
                validator.ValidateSequence(
                    [
                        new Statement.BeginTransaction(new BeginTransactionStatement { Name = "tx" }),
                        new Statement.BeginTransaction(new BeginTransactionStatement { Name = "tx" }),
                    ]
                )
        );

        Assert.Throws<ValidationException.TransactionNotFoundException>(
            () =>
                validator.ValidateSequence(
                    [
                        new Statement.BeginTransaction(new BeginTransactionStatement { Name = "tx" }),
                        new Statement.Commit(
                            new CommitStatement(new CommitScope.Named("missing"), chain: false)
                        ),
                    ]
                )
        );

        Assert.Throws<ValidationException.TransactionNestingViolationException>(
            () =>
                validator.ValidateSequence(
                    [
                        new Statement.BeginTransaction(new BeginTransactionStatement { Name = "outer" }),
                        new Statement.BeginTransaction(new BeginTransactionStatement { Name = "inner" }),
                        new Statement.Rollback(
                            new RollbackStatement(
                                new RollbackScope.Named("outer"),
                                chain: false
                            )
                        ),
                    ]
                )
        );

        validator.ValidateSequence(
            [
                new Statement.BeginTransaction(new BeginTransactionStatement { Name = "tx_ok" }),
                new Statement.Savepoint(new SavepointStatement("sp_ok")),
                new Statement.ReleaseSavepoint(new ReleaseSavepointStatement("sp_ok")),
                new Statement.Commit(new CommitStatement(CommitScope.Current.Instance, chain: false)),
            ]
        );
    }

    [Fact]
    public void SqlValidator_Validate_CoversSelectInsertEnumAndViewAsChecks()
    {
        var catalog = new Catalog();
        catalog.CreateTable(
            new TableSchema(
                "users",
                [
                    new ColumnSchema("id", DataType.BigInt.Instance, nullable: false),
                    new ColumnSchema("name", new DataType.Varchar(128), nullable: true),
                ]
            )
        );
        var validator = new SqlValidator(catalog);

        validator.Validate(
            new Statement.Select(
                new SelectStatement
                {
                    Columns = [new SelectItem.QualifiedWildcard("users")],
                    From = new TableReference.Named(null, null, "users", alias: "u"),
                    WhereClause = new Expr.Column("u", "id"),
                    GroupBy = [new Expr.Column("u", "id")],
                    Having = new Expr.Literal(new SqlValue.Integer(1)),
                    OrderBy = [new OrderByExpr(new Expr.Column("u", "id"), ascending: true)],
                }
            )
        );

        Assert.Throws<ValidationException.InvalidAggregateUsageException>(
            () =>
                validator.Validate(
                    new Statement.Select(
                        new SelectStatement
                        {
                            Columns = [SelectItem.Wildcard.Instance],
                            From = new TableReference.Named(null, null, "users", alias: null),
                            WhereClause = new Expr.Function(
                                "SUM",
                                [new Expr.Column(null, "id")],
                                distinct: false
                            ),
                        }
                    )
                )
        );

        Assert.Throws<ValidationException.ViewAsAggregateNotAllowedException>(
            () =>
                validator.Validate(
                    new Statement.Select(
                        new SelectStatement
                        {
                            Columns = [SelectItem.Wildcard.Instance],
                            From = new TableReference.Named(null, null, "users", alias: null),
                            ViewAs =
                            [
                                new ViewAsItem(
                                    new Expr.Function("COUNT", [Expr.Wildcard.Instance], false),
                                    "c"
                                ),
                            ],
                        }
                    )
                )
        );

        Assert.Throws<ValidationException.ViewAsSubqueryNotAllowedException>(
            () =>
                validator.Validate(
                    new Statement.Select(
                        new SelectStatement
                        {
                            Columns = [SelectItem.Wildcard.Instance],
                            From = new TableReference.Named(null, null, "users", alias: null),
                            ViewAs =
                            [
                                new ViewAsItem(
                                    new Expr.Subquery(
                                        new SelectStatement
                                        {
                                            Columns =
                                            [
                                                new SelectItem.ExprItem(
                                                    new Expr.Literal(new SqlValue.Integer(1)),
                                                    alias: null
                                                ),
                                            ],
                                        }
                                    ),
                                    "s"
                                ),
                            ],
                        }
                    )
                )
        );

        Assert.Throws<ValidationException.NullConstraintViolationException>(
            () =>
                validator.Validate(
                    new Statement.Insert(
                        new InsertStatement(
                            "users",
                            ["id"],
                            [[new Expr.Literal(SqlValue.Null.Instance)]]
                        )
                    )
                )
        );

        Assert.Throws<ValidationException.ConstraintViolationException>(
            () =>
                validator.Validate(
                    new Statement.CreateEnum(
                        new CreateEnumStatement("status", flag: false, variants: [], ifNotExists: false)
                    )
                )
        );
    }

    [Fact]
    public void Catalog_BranchesAndIndexTypeParsing_AreCovered()
    {
        var catalog = new Catalog();
        catalog.CreateTable(
            new TableSchema(
                "users",
                [new ColumnSchema("id", DataType.BigInt.Instance, nullable: false)]
            )
        );

        var duplicateEx = Assert.Throws<PlannerException>(
            () =>
                catalog.CreateTable(
                    new TableSchema(
                        "users",
                        [new ColumnSchema("id", DataType.BigInt.Instance, nullable: false)]
                    )
                )
        );
        Assert.Equal(PlannerErrorKind.CatalogError, duplicateEx.Kind);

        var missingTableDropEx = Assert.Throws<PlannerException>(() => catalog.DropTable("missing"));
        Assert.Equal(PlannerErrorKind.CatalogError, missingTableDropEx.Kind);

        var missingIndexTableEx = Assert.Throws<PlannerException>(
            () =>
                catalog.CreateIndex(
                    new CreateIndexStatement
                    {
                        Name = "missing_idx",
                        Table = "missing",
                        Columns = [new IndexColumn("id", true)],
                        Unique = false,
                        IndexType = IndexType.BTree.Instance,
                    }
                )
        );
        Assert.Equal(PlannerErrorKind.CatalogError, missingIndexTableEx.Kind);

        var missingColumnEx = Assert.Throws<PlannerException>(
            () =>
                catalog.CreateIndex(
                    new CreateIndexStatement
                    {
                        Name = "users_missing_col_idx",
                        Table = "users",
                        Columns = [new IndexColumn("missing_col", true)],
                        Unique = false,
                        IndexType = IndexType.BTree.Instance,
                    }
                )
        );
        Assert.Equal(PlannerErrorKind.CatalogError, missingColumnEx.Kind);

        catalog.CreateIndex(
            new CreateIndexStatement
            {
                Name = "users_id_idx",
                Table = "users",
                Columns = [new IndexColumn("id", true)],
                Unique = true,
                IndexType = IndexType.BTree.Instance,
            }
        );

        var duplicateIndexEx = Assert.Throws<PlannerException>(
            () =>
                catalog.CreateIndex(
                    new CreateIndexStatement
                    {
                        Name = "users_id_idx",
                        Table = "users",
                        Columns = [new IndexColumn("id", true)],
                        Unique = true,
                        IndexType = IndexType.BTree.Instance,
                    }
                )
        );
        Assert.Equal(PlannerErrorKind.CatalogError, duplicateIndexEx.Kind);

        var missingDropIndexEx = Assert.Throws<PlannerException>(() => catalog.DropIndex("missing_idx"));
        Assert.Equal(PlannerErrorKind.CatalogError, missingDropIndexEx.Kind);

        var fileName = $"aeternum-catalog-index-types-{Guid.NewGuid():N}.json";
        var path = Path.GetFullPath(fileName, Path.GetTempPath());
        try
        {
            const string state = """
                {
                  "Tables": [
                    {
                      "Name": "idx_table",
                      "SchemaVersion": 1,
                      "CreatedAt": "2026-01-01T00:00:00+00:00",
                      "ModifiedAt": "2026-01-01T00:00:00+00:00",
                      "RowCount": 0,
                      "Columns": [
                        { "Name": "id", "DataTypeText": "BIGINT", "Nullable": false }
                      ]
                    }
                  ],
                  "Types": [],
                  "Indexes": [
                    { "Name": "idx_hash", "Table": "idx_table", "Columns": ["id"], "Unique": false, "IndexType": "HASH", "CreatedAt": "2026-01-01T00:00:00+00:00" },
                    { "Name": "idx_unknown", "Table": "idx_table", "Columns": ["id"], "Unique": false, "IndexType": "BTREE_CUSTOM", "CreatedAt": "2026-01-01T00:00:00+00:00" }
                  ],
                  "NextObjectId": 2
                }
                """;
            File.WriteAllText(path, state);

            var reloaded = new Catalog(path);
            var hash = reloaded.GetTableIndexes("idx_table").Single(i => i.Name == "idx_hash");
            var unknown = reloaded.GetTableIndexes("idx_table").Single(i => i.Name == "idx_unknown");
            Assert.IsType<IndexType.Hash>(hash.IndexType);
            Assert.IsType<IndexType.Other>(unknown.IndexType);
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
    public void SqlValidator_AdditionalBranches_AreCovered()
    {
        var catalog = new Catalog();
        catalog.CreateTable(
            new TableSchema(
                "users",
                [
                    new ColumnSchema("id", DataType.BigInt.Instance, nullable: false),
                    new ColumnSchema("status", new DataType.EnumRef("status"), nullable: false),
                ]
            )
        );
        var validator = new SqlValidator(catalog);

        Assert.Throws<ValidationException.TypeNotFoundException>(
            () =>
                validator.Validate(
                    new Statement.CreateTable(
                        new CreateTableStatement
                        {
                            Table = "tickets",
                            Columns =
                            [
                                new ColumnDef
                                {
                                    Name = "state",
                                    DataType = new DataType.EnumRef("missing_type"),
                                    Nullable = false,
                                },
                            ],
                        }
                    )
                )
        );

        catalog.AddType(
            new UserTypeSchema(
                "status",
                new UserTypeKind.Enum(false, [new EnumVariant("OPEN")], [1])
            )
        );

        validator.Validate(
            new Statement.CreateTable(
                new CreateTableStatement
                {
                    Table = "tickets",
                    Columns =
                    [
                        new ColumnDef
                        {
                            Name = "state",
                            DataType = new DataType.EnumRef("status"),
                            Nullable = false,
                        },
                    ],
                }
            )
        );

        Assert.Throws<ValidationException.NullConstraintViolationException>(
            () =>
                validator.Validate(
                    new Statement.Update(
                        new UpdateStatement(
                            "users",
                            [("id", new Expr.Literal(SqlValue.Null.Instance))],
                            whereClause: null
                        )
                    )
                )
        );

        validator.Validate(
            new Statement.Delete(new DeleteStatement("users", new Expr.Column(null, "id")))
        );

        Assert.Throws<ValidationException.ColumnNotFoundException>(
            () =>
                validator.Validate(
                    new Statement.AlterTable(
                        new AlterTableStatement(
                            "users",
                            [new AlterTableOperation.RenameColumn("missing", "new_name")]
                        )
                    )
                )
        );

        Assert.Throws<ValidationException.TypeNotFoundException>(
            () => validator.Validate(new Statement.DropEnum(new DropEnumStatement("missing", ifExists: false)))
        );

        Assert.Throws<ValidationException.TypeInUseException>(
            () => validator.Validate(new Statement.DropEnum(new DropEnumStatement("status", ifExists: false)))
        );
    }
}
