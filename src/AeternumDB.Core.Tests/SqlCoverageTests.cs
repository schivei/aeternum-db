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
    public void SqlCoverage_AdditionalBranchPaths_AreCoveredDeterministically()
    {
        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"reference","DataId":"users_data_id"}""",
                out var referenceFromDataId
            )
        );
        Assert.Equal("users_data_id", Assert.IsType<DataType.Reference>(referenceFromDataId).Table);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"reference_array","DataId":"users_array_data_id"}""",
                out var referenceArrayFromDataId
            )
        );
        Assert.Equal(
            "users_array_data_id",
            Assert.IsType<DataType.ReferenceArray>(referenceArrayFromDataId).Table
        );

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"user_defined","DataId":"status_data_id"}""",
                out var enumFromDataId
            )
        );
        Assert.Equal("status_data_id", Assert.IsType<DataType.EnumRef>(enumFromDataId).Name);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"reference"}""",
                out var referenceFallback
            )
        );
        Assert.Equal(
            CatalogDataTypePersistenceConstants.UnknownTypeName,
            Assert.IsType<DataType.Reference>(referenceFallback).Table
        );

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"reference_array"}""",
                out var referenceArrayFallback
            )
        );
        Assert.Equal(
            CatalogDataTypePersistenceConstants.UnknownTypeName,
            Assert.IsType<DataType.ReferenceArray>(referenceArrayFallback).Table
        );

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"user_defined"}""",
                out var enumFallback
            )
        );
        Assert.Equal(
            CatalogDataTypePersistenceConstants.UnknownTypeName,
            Assert.IsType<DataType.EnumRef>(enumFallback).Name
        );

        var catalog = new Catalog();
        catalog.CreateTable(
            new TableSchema(
                "users",
                [
                    new ColumnSchema("id", DataType.BigInt.Instance, nullable: false),
                    new ColumnSchema("name", new DataType.Varchar(32), nullable: true),
                ]
            )
        );
        catalog.CreateTable(
            new TableSchema(
                "other",
                [new ColumnSchema("id", DataType.BigInt.Instance, nullable: false)]
            )
        );

        Assert.True(
            catalog.CreateIndex(
                new CreateIndexStatement
                {
                    Name = null,
                    Table = "users",
                    Columns = [new IndexColumn("id", ascending: true)],
                    Unique = false,
                    IfNotExists = false,
                }
            )
        );
        Assert.Contains(catalog.GetTableIndexes("users"), i => i.Name == "users_id_idx");

        catalog.AddIndex(
            new IndexSchema(
                "users_multi_idx",
                "users",
                ["id", "name"],
                unique: false,
                IndexType.BTree.Instance
            )
        );
        catalog.AddIndex(
            new IndexSchema(
                "users_name_idx",
                "users",
                ["name"],
                unique: false,
                IndexType.BTree.Instance
            )
        );
        catalog.AddIndex(
            new IndexSchema(
                "other_id_idx",
                "other",
                ["id"],
                unique: false,
                IndexType.BTree.Instance
            )
        );
        catalog.AlterTable(
            new AlterTableStatement("users", [new AlterTableOperation.RenameColumn("id", "id2")])
        );

        var usersIndexes = catalog.GetTableIndexes("users");
        Assert.Contains(usersIndexes, i => i.Name == "users_multi_idx" && i.Columns.SequenceEqual(["id2", "name"]));
        Assert.Contains(usersIndexes, i => i.Name == "users_name_idx" && i.Columns.SequenceEqual(["name"]));
        Assert.Contains(catalog.GetTableIndexes("other"), i => i.Name == "other_id_idx");

        var validator = new SqlValidator(catalog);
        var qualifiedAliasException = Record.Exception(
            () =>
                validator.Validate(
                    new Statement.Select(
                        new SelectStatement
                        {
                            Columns = [new SelectItem.QualifiedWildcard("u")],
                            From = new TableReference.Named(null, null, "users", alias: "u"),
                        }
                    )
                )
        );
        Assert.Null(qualifiedAliasException);

        var wildcardException = Record.Exception(
            () =>
                validator.Validate(
                    new Statement.Select(
                        new SelectStatement
                        {
                            Columns = [SelectItem.Wildcard.Instance],
                            From = new TableReference.Named(null, null, "users", alias: "u"),
                        }
                    )
                )
        );
        Assert.Null(wildcardException);

        var expandException = Record.Exception(
            () =>
                validator.Validate(
                    new Statement.Select(
                        new SelectStatement
                        {
                            Columns = [new SelectItem.Expand(new Expr.Column("u", "id2"), "id_alias")],
                            From = new TableReference.Named(null, null, "users", alias: "u"),
                        }
                    )
                )
        );
        Assert.Null(expandException);

        var nullSelectItemException = Record.Exception(
            () =>
                validator.Validate(
                    new Statement.Select(
                        new SelectStatement
                        {
                            Columns = [null!],
                            From = new TableReference.Named(null, null, "users", alias: "u"),
                        }
                    )
                )
        );
        Assert.Null(nullSelectItemException);

        var emptyAlterOperationsException = Record.Exception(
            () =>
                validator.Validate(
                    new Statement.AlterTable(new AlterTableStatement("users", []))
                )
        );
        Assert.Null(emptyAlterOperationsException);

        var anonymousStackException = Record.Exception(
            () =>
                validator.ValidateSequence(
                    [
                        new Statement.BeginTransaction(new BeginTransactionStatement()),
                        new Statement.BeginTransaction(new BeginTransactionStatement { Name = "named_tx" }),
                        new Statement.Commit(new CommitStatement(CommitScope.Current.Instance, chain: false)),
                        new Statement.Commit(new CommitStatement(CommitScope.Current.Instance, chain: false)),
                    ]
                )
        );
        Assert.Null(anonymousStackException);

        var nullSnapshotPath = Path.Join(
            Path.GetTempPath(),
            $"aeternum-catalog-null-snapshot-{Guid.NewGuid():N}.json"
        );
        try
        {
            File.WriteAllText(nullSnapshotPath, "null");
            var loadedCatalog = new Catalog(nullSnapshotPath);
            Assert.False(loadedCatalog.TableExists("users"));
        }
        finally
        {
            if (File.Exists(nullSnapshotPath))
                File.Delete(nullSnapshotPath);
            if (File.Exists($"{nullSnapshotPath}.tmp"))
                File.Delete($"{nullSnapshotPath}.tmp");
        }
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

        var commitNestingException = Assert.Throws<ValidationException.TransactionNestingViolationException>(
            () =>
                validator.ValidateSequence(
                    [
                        new Statement.BeginTransaction(new BeginTransactionStatement { Name = "outer_commit" }),
                        new Statement.BeginTransaction(new BeginTransactionStatement { Name = "inner_commit" }),
                        new Statement.Commit(
                            new CommitStatement(new CommitScope.Named("outer_commit"), chain: false)
                        ),
                    ]
                )
        );
        Assert.Equal("outer_commit", commitNestingException.Target);
        Assert.Equal("inner_commit", commitNestingException.Blocking);

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

    [Fact]
    public void Ast_ComprehensiveNodeConstruction_CoversStatementAndExpressionShapes()
    {
        var literalExpr = new Expr.Literal(new SqlValue.Integer(7));
        var tableColumn = new Expr.Column("users", "id");
        var plainColumn = new Expr.Column(null, "name");
        var unaryExpr = new Expr.UnaryOp(UnaryOperator.Not, plainColumn);
        var binaryExpr = new Expr.BinaryOp(tableColumn, BinaryOperator.Eq, literalExpr);
        var functionExpr = new Expr.Function("LOWER", [plainColumn], distinct: false);
        var isNullExpr = new Expr.IsNull(plainColumn, negated: false);
        var betweenExpr = new Expr.Between(plainColumn, new Expr.Literal(new SqlValue.Integer(1)), new Expr.Literal(new SqlValue.Integer(9)), negated: false);
        var inListExpr = new Expr.InList(plainColumn, [new Expr.Literal(new SqlValue.SqlString("a")), new Expr.Literal(new SqlValue.SqlString("b"))], negated: false);
        var castExpr = new Expr.Cast(plainColumn, DataType.Integer.Instance);
        var caseExpr = new Expr.Case(
            operand: plainColumn,
            conditions: [(new Expr.Literal(new SqlValue.Boolean(true)), new Expr.Literal(new SqlValue.SqlString("ok")))],
            elseResult: new Expr.Literal(new SqlValue.SqlString("fallback"))
        );
        var substringExpr = new Expr.Substring(plainColumn, new Expr.Literal(new SqlValue.Integer(1)), new Expr.Literal(new SqlValue.Integer(2)));
        var trimExpr = new Expr.Trim(plainColumn, TrimWhereField.Both, new Expr.Literal(new SqlValue.SqlString(" ")));
        var arrayExpr = new Expr.ArrayOp(plainColumn, BinaryOperator.Eq, ArrayQuantifier.Any, new Expr.Literal(new SqlValue.Integer(1)));
        var overlayExpr = new Expr.Overlay(plainColumn, new Expr.Literal(new SqlValue.SqlString("x")), new Expr.Literal(new SqlValue.Integer(1)), new Expr.Literal(new SqlValue.Integer(1)));
        var positionExpr = new Expr.Position(new Expr.Literal(new SqlValue.SqlString("a")), plainColumn);
        var matchAgainstExpr = new Expr.MatchAgainst(["name"], new Expr.Literal(new SqlValue.SqlString("term")), TextSearchModifier.Boolean);

        var subquery = new SelectStatement
        {
            Columns = [new SelectItem.ExprItem(new Expr.Literal(new SqlValue.Integer(1)), "one")],
            From = new TableReference.Named(null, null, "users", alias: "u"),
            WhereClause = new Expr.BinaryOp(new Expr.Column("u", "id"), BinaryOperator.Gt, new Expr.Literal(new SqlValue.Integer(0))),
            GroupBy = [new Expr.Column("u", "id")],
            Having = new Expr.BinaryOp(new Expr.Column("u", "id"), BinaryOperator.Gt, new Expr.Literal(new SqlValue.Integer(0))),
            OrderBy = [new OrderByExpr(new Expr.Column("u", "id"), ascending: true)],
            Limit = 10,
            Offset = 0,
            Distinct = true,
            ViewAs = [new ViewAsItem(new Expr.Column("u", "id"), "id_alias")],
        };

        var inSubqueryExpr = new Expr.InSubquery(plainColumn, subquery, negated: false);
        var subqueryExpr = new Expr.Subquery(subquery);
        var selectExpand = new SelectItem.Expand(matchAgainstExpr, "match");
        var qualifiedWildcard = new SelectItem.QualifiedWildcard("users");
        var cte = new CommonTableExpr("cte_users", ["id"], subquery);
        var tableRefSubquery = new TableReference.Subquery(subquery, "sq");
        var tableRefJoin = new TableReference.Join(
            new TableReference.Named(null, null, "users", alias: "u"),
            new TableReference.Named(null, null, "users", alias: "u2"),
            JoinType.Inner,
            new Expr.BinaryOp(new Expr.Column("u", "id"), BinaryOperator.Eq, new Expr.Column("u2", "id"))
        );

        var createTable = new CreateTableStatement
        {
            Database = "db",
            Schema = "public",
            Table = "users",
            Columns =
            [
                new ColumnDef
                {
                    Name = "id",
                    DataType = DataType.BigInt.Instance,
                    Nullable = false,
                    PrimaryKey = true,
                    Unique = true,
                    Default = new Expr.Literal(new SqlValue.Integer(1)),
                    AutoIncrement = true,
                    MinLength = 1,
                    MaxLength = 32,
                    RequiresDistinctReferences = true,
                    Check = new Expr.BinaryOp(new Expr.Column(null, "id"), BinaryOperator.Gt, new Expr.Literal(new SqlValue.Integer(0))),
                    TextDirective = new TextDirective("en-US"),
                    TermsDirectives = [new TermsDirective("terms", TermsDirectiveKind.Text)],
                    OnUpdate = ReferentialAction.Cascade,
                    OnDelete = ReferentialAction.Restrict,
                },
            ],
            IfNotExists = true,
            Temporary = true,
            Inherits = ["base_table"],
            OnCommit = OnCommitBehavior.PreserveRows,
            Constraints =
            [
                new TableConstraint.PrimaryKey("pk_users", ["id"]),
                new TableConstraint.Unique("uq_users_id", ["id"]),
                new TableConstraint.Check("ck_users_id", new Expr.BinaryOp(new Expr.Column(null, "id"), BinaryOperator.Gt, new Expr.Literal(new SqlValue.Integer(0)))),
            ],
            Versioned = true,
            Flat = false,
        };

        var createIndex = new CreateIndexStatement
        {
            Name = "users_id_idx",
            Table = "users",
            Columns = [new IndexColumn("id", true)],
            Unique = false,
            IfNotExists = true,
            IndexType = IndexType.BTree.Instance,
        };
        var dropIndex = new DropIndexStatement(["users_id_idx"], ifExists: true);
        var alterTable = new AlterTableStatement(
            "users",
            [
                new AlterTableOperation.AddColumn(new ColumnDef { Name = "email", DataType = new DataType.Varchar(128), Nullable = true }),
                new AlterTableOperation.DropColumn("email", ifExists: true),
                new AlterTableOperation.RenameColumn("id", "user_id"),
                new AlterTableOperation.RenameTable("users_v2"),
            ]
        );

        var statements = new Statement[]
        {
            new Statement.Select(new SelectStatement
            {
                With = [cte],
                Columns = [SelectItem.Wildcard.Instance, qualifiedWildcard, selectExpand],
                From = tableRefJoin,
                WhereClause = binaryExpr,
                GroupBy = [plainColumn],
                Having = functionExpr,
                OrderBy = [new OrderByExpr(plainColumn, ascending: false)],
                Distinct = false,
            }),
            new Statement.Insert(new InsertStatement("users", ["id"], [[new Expr.Literal(new SqlValue.Integer(1))]])),
            new Statement.Update(new UpdateStatement("users", [("id", new Expr.Literal(new SqlValue.Integer(2)))], whereClause: new Expr.Column(null, "id"))),
            new Statement.Delete(new DeleteStatement("users", whereClause: new Expr.Column(null, "id"))),
            new Statement.CreateTable(createTable),
            new Statement.DropTable(new DropTableStatement(["users"], ifExists: true)),
            new Statement.AlterTable(alterTable),
            new Statement.Grant(new GrantStatement(["SELECT"], ["id"], "users", ["reader"])),
            new Statement.Revoke(new RevokeStatement(["SELECT"], ["id"], "users", ["reader"])),
            new Statement.CreateMaterializedView(new CreateMaterializedViewStatement("mv_users", subquery, ifNotExists: true, orReplace: false)),
            new Statement.BeginTransaction(new BeginTransactionStatement { Name = "tx", IsolationLevel = IsolationLevel.Serializable, ReadOnly = true }),
            new Statement.Commit(new CommitStatement(new CommitScope.Named("tx"), chain: false)),
            new Statement.Rollback(new RollbackStatement(new RollbackScope.ToSavepoint("sp"), chain: false)),
            new Statement.Savepoint(new SavepointStatement("sp")),
            new Statement.ReleaseSavepoint(new ReleaseSavepointStatement("sp")),
            new Statement.CreateIndex(createIndex),
            new Statement.DropIndex(dropIndex),
            new Statement.CreateUser(new CreateUserStatement("alice", "secret", ["admin"])),
            new Statement.DropUser(new DropUserStatement(["alice"], ifExists: true)),
            new Statement.CreateEnum(new CreateEnumStatement("status", flag: false, variants: [new EnumVariant("OPEN")], ifNotExists: true)),
            new Statement.DropEnum(new DropEnumStatement("status", ifExists: true)),
            new Statement.CreateType(new CreateTypeStatement("my_type", new TypeDefinition.Composite([new CompositeField("id", DataType.BigInt.Instance, true)]))),
            new Statement.DropType(new DropTypeStatement("my_type", ifExists: true)),
            new Statement.CreateDatabase(new CreateDatabaseStatement("db", ifNotExists: true)),
            new Statement.DropDatabase(new DropDatabaseStatement("db", ifExists: true)),
            new Statement.UseDatabase(new UseDatabaseStatement("db")),
            new Statement.CreateSchema(new CreateSchemaStatement("db", "public", ifNotExists: true)),
            new Statement.DropSchema(new DropSchemaStatement("db", "public", ifExists: true)),
        };

        Assert.Equal("NULL", SqlValue.Null.Instance.ToString());
        Assert.Equal("7", new SqlValue.Integer(7).ToString());
        Assert.Equal("TRUE", new SqlValue.Boolean(true).ToString());
        var selectFromJoin = Assert.IsType<TableReference.Join>(((Statement.Select)statements[0]).Query.From);
        var joinLeft = Assert.IsType<TableReference.Named>(selectFromJoin.Left);
        Assert.Equal("users", joinLeft.Name);
        Assert.Equal("mv_users", ((Statement.CreateMaterializedView)statements[9]).Stmt.Name);
        Assert.Equal("users_id_idx", ((Statement.DropIndex)statements[16]).Stmt.Names.Single());
        Assert.Equal("db", ((Statement.CreateSchema)statements[26]).Stmt.Database);
        Assert.Equal("term", ((SqlValue.SqlString)((Expr.Literal)matchAgainstExpr.MatchValue).Value).Value);
        Assert.Equal("a", ((SqlValue.SqlString)((Expr.Literal)positionExpr.Substr).Value).Value);
        Assert.Equal(BinaryOperator.Eq, arrayExpr.Op);
        Assert.False(isNullExpr.Negated);
        Assert.False(betweenExpr.Negated);
        Assert.False(inListExpr.Negated);
        Assert.False(inSubqueryExpr.Negated);
        Assert.NotNull(subqueryExpr.Query);
        Assert.Equal("one", ((SelectItem.ExprItem)subquery.Columns.Single()).Alias);
        Assert.NotNull(tableRefSubquery.Query);
        Assert.NotNull(unaryExpr.Inner);
        Assert.NotNull(castExpr.DataType);
        Assert.NotNull(caseExpr.ElseResult);
        Assert.NotNull(substringExpr.Len);
        Assert.NotNull(trimExpr.TrimWhere);
        Assert.NotNull(overlayExpr.ForLen);
        Assert.Same(RollbackScope.All.Instance, RollbackScope.All.Instance);
        Assert.Equal(28, statements.Length);
    }

    [Fact]
    public void Catalog_AdditionalPersistenceAndWrapperBranches_AreCovered()
    {
        var rootDir = Path.Join(Path.GetTempPath(), $"aeternum-catalog-branch-{Guid.NewGuid():N}");
        var path = Path.Join(rootDir, "meta", "catalog.json");
        try
        {
            var catalog = new Catalog(path);
            catalog.CreateTable(
                new TableSchema("users", [new ColumnSchema("id", DataType.BigInt.Instance, nullable: false)])
            );

            catalog.AddType(
                new UserTypeSchema(
                    "composite_profile",
                    new UserTypeKind.Composite([("id", DataType.BigInt.Instance), ("name", new DataType.Varchar(50))])
                )
            );

            catalog.AddIndex(new IndexSchema("idx_hash", "users", ["id"], unique: false, IndexType.Hash.Instance));
            catalog.AddIndex(new IndexSchema("idx_gin", "users", ["id"], unique: false, IndexType.Gin.Instance));
            catalog.AddIndex(new IndexSchema("idx_gist", "users", ["id"], unique: false, IndexType.Gist.Instance));
            catalog.AddIndex(new IndexSchema("idx_spgist", "users", ["id"], unique: false, IndexType.SpGist.Instance));
            catalog.AddIndex(new IndexSchema("idx_brin", "users", ["id"], unique: false, IndexType.Brin.Instance));
            catalog.AddIndex(new IndexSchema("idx_bloom", "users", ["id"], unique: false, IndexType.Bloom.Instance));
            catalog.AddIndex(new IndexSchema("idx_fulltext", "users", ["id"], unique: false, IndexType.FullText.Instance));
            catalog.AddIndex(new IndexSchema("idx_trigram", "users", ["id"], unique: false, IndexType.Trigram.Instance));
            catalog.AddIndex(new IndexSchema("idx_other", "users", ["id"], unique: false, new IndexType.Other("CUSTOM_KIND")));

            catalog.Save();
            var snapshotPath = Path.Join(rootDir, "meta", "catalog_snapshot.json");
            File.Copy(path, snapshotPath, overwrite: true);
            catalog.DropIndex(new DropIndexStatement(["idx_hash"], ifExists: true));
            catalog.DropTable(new DropTableStatement(["users"], ifExists: true));
            catalog.Load();

            Assert.False(catalog.TableExists("users"));
            Assert.Empty(catalog.GetTableIndexes("users"));

            var snapshotCatalog = new Catalog(snapshotPath);
            Assert.True(snapshotCatalog.TableExists("users"));
            Assert.Contains(snapshotCatalog.GetTableIndexes("users"), i => i.Name == "idx_gin");
            Assert.Contains(snapshotCatalog.GetTableIndexes("users"), i => i.Name == "idx_other");

            var catalogTmpPath = Path.Join(rootDir, "meta", "catalog_from_tmp.json");
            var tmpOnly = $"{catalogTmpPath}.tmp";
            const string tmpState = """
                {
                  "Tables": [
                    {
                      "Name": "tmp_users",
                      "SchemaVersion": 1,
                      "CreatedAt": "2026-01-01T00:00:00+00:00",
                      "ModifiedAt": "2026-01-01T00:00:00+00:00",
                      "RowCount": 0,
                      "Columns": [
                        { "Name": "id", "DataTypeText": "BIGINT", "Nullable": false }
                      ]
                    }
                  ],
                  "Types": [
                    {
                      "Name": "tmp_enum",
                      "Kind": "enum",
                      "Flag": false,
                      "Variants": [ { "Name": "A", "IsNone": false } ],
                      "ResolvedValues": [ 1 ]
                    }
                  ],
                  "Indexes": [],
                  "NextObjectId": 2
                }
                """;
            File.WriteAllText(tmpOnly, tmpState);
            var loadedFromTmp = new Catalog(catalogTmpPath);
            Assert.True(loadedFromTmp.TableExists("tmp_users"));
            Assert.True(loadedFromTmp.TypeExists("tmp_enum"));
        }
        finally
        {
            if (Directory.Exists(rootDir))
                Directory.Delete(rootDir, recursive: true);
        }
    }

    [Fact]
    public void SqlValidator_ComprehensiveExpressionsAndSequences_AreCovered()
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
        catalog.CreateTable(
            new TableSchema("logs", [new ColumnSchema("id", DataType.BigInt.Instance, nullable: false)])
        );

        var validator = new SqlValidator(catalog);
        var baseExpr = new Expr.Column(null, "id");
        var whereExpr = new Expr.BinaryOp(
            new Expr.UnaryOp(UnaryOperator.Not, new Expr.IsNull(baseExpr, negated: false)),
            BinaryOperator.And,
            new Expr.Between(baseExpr, new Expr.Literal(new SqlValue.Integer(1)), new Expr.Literal(new SqlValue.Integer(10)), negated: false)
        );

        validator.Validate(
            new Statement.Select(
                new SelectStatement
                {
                    Columns =
                    [
                        new SelectItem.ExprItem(new Expr.Trim(new Expr.Column(null, "name"), TrimWhereField.Both, new Expr.Literal(new SqlValue.SqlString(" "))), "trimmed_name"),
                        new SelectItem.Expand(
                            new Expr.Substring(new Expr.Column(null, "name"), new Expr.Literal(new SqlValue.Integer(1)), new Expr.Literal(new SqlValue.Integer(3))),
                            "name_prefix"
                        ),
                    ],
                    From = new TableReference.Join(
                        new TableReference.Named(null, null, "users", alias: "u"),
                        new TableReference.Subquery(
                            new SelectStatement
                            {
                                Columns = [new SelectItem.ExprItem(new Expr.Literal(new SqlValue.Integer(1)), "id")],
                                From = new TableReference.Named(null, null, "logs", alias: "l"),
                            },
                            "sq"
                        ),
                        JoinType.Left,
                        filterBy: new Expr.BinaryOp(new Expr.Column("u", "id"), BinaryOperator.Eq, new Expr.Literal(new SqlValue.Integer(1)))
                    ),
                    WhereClause = new Expr.BinaryOp(
                        whereExpr,
                        BinaryOperator.Or,
                        new Expr.Case(
                            operand: null,
                            conditions: [(new Expr.Literal(new SqlValue.Boolean(true)), new Expr.Literal(new SqlValue.Boolean(true)))],
                            elseResult: null
                        )
                    ),
                    GroupBy = [new Expr.Column("u", "id")],
                    Having = new Expr.BinaryOp(new Expr.Function("MAX", [new Expr.Column("u", "id")], false), BinaryOperator.Gt, new Expr.Literal(new SqlValue.Integer(0))),
                    OrderBy = [new OrderByExpr(new Expr.Cast(new Expr.Column("u", "id"), DataType.Integer.Instance), ascending: false)],
                }
            )
        );

        validator.Validate(
            new Statement.Update(
                new UpdateStatement(
                    "users",
                    [("name", new Expr.Literal(new SqlValue.SqlString("changed")))],
                    whereClause: new Expr.InSubquery(
                        new Expr.Column(null, "id"),
                        new SelectStatement
                        {
                            Columns = [new SelectItem.ExprItem(new Expr.Column(null, "id"), "id")],
                            From = new TableReference.Named(null, null, "users", alias: null),
                        },
                        negated: false
                    )
                )
            )
        );

        validator.Validate(
            new Statement.Insert(
                new InsertStatement(
                    "users",
                    ["id", "name"],
                    [[new Expr.Literal(new SqlValue.Integer(1)), new Expr.Literal(new SqlValue.SqlString("n"))]]
                )
            )
        );

        validator.ValidateSequence(
            [
                new Statement.BeginTransaction(new BeginTransactionStatement { Name = "outer" }),
                new Statement.BeginTransaction(new BeginTransactionStatement { Name = "inner" }),
                new Statement.Rollback(new RollbackStatement(RollbackScope.Current.Instance, chain: false)),
                new Statement.Commit(new CommitStatement(CommitScope.All.Instance, chain: false)),
            ]
        );

        validator.ValidateSequence(
            [
                new Statement.BeginTransaction(new BeginTransactionStatement { Name = "tx2" }),
                new Statement.Rollback(new RollbackStatement(new RollbackScope.ToSavepoint("sp"), chain: false)),
                new Statement.Commit(new CommitStatement(CommitScope.Current.Instance, chain: false)),
            ]
        );

        Assert.True(catalog.TableExists("users"));
    }

    [Fact]
    public void Catalog_AdditionalErrorBranches_AreCovered()
    {
        var catalog = new Catalog();
        catalog.CreateTable(
            new TableSchema("users", [new ColumnSchema("id", DataType.BigInt.Instance, nullable: false)])
        );

        Assert.False(
            catalog.CreateTable(
                new TableSchema("users", [new ColumnSchema("id", DataType.BigInt.Instance, nullable: false)]),
                ifNotExists: true
            )
        );

        Assert.Throws<PlannerException>(
            () =>
                catalog.AlterTable(
                    new AlterTableStatement("missing", [new AlterTableOperation.RenameTable("x")])
                )
        );

        catalog.CreateIndex(
            new CreateIndexStatement
            {
                Name = "users_id_idx",
                Table = "users",
                Columns = [new IndexColumn("id", true)],
                Unique = false,
                IndexType = IndexType.BTree.Instance,
            }
        );

        Assert.False(
            catalog.CreateIndex(
                new CreateIndexStatement
                {
                    Name = "users_id_idx",
                    Table = "users",
                    Columns = [new IndexColumn("id", true)],
                    Unique = false,
                    IfNotExists = true,
                    IndexType = IndexType.BTree.Instance,
                }
            )
        );

        Assert.Throws<PlannerException>(
            () =>
                catalog.AlterTable(
                    new AlterTableStatement(
                        "users",
                        [new AlterTableOperation.AddColumn(new ColumnDef { Name = "id", DataType = DataType.Integer.Instance, Nullable = true })]
                    )
                )
        );

        Assert.Throws<PlannerException>(
            () =>
                catalog.AlterTable(
                    new AlterTableStatement(
                        "users",
                        [new AlterTableOperation.DropColumn("missing", ifExists: false)]
                    )
                )
        );

        Assert.Throws<PlannerException>(
            () =>
                catalog.AlterTable(
                    new AlterTableStatement(
                        "users",
                        [new AlterTableOperation.RenameColumn("missing", "id2")]
                    )
                )
        );

        catalog.AlterTable(
            new AlterTableStatement(
                "users",
                [new AlterTableOperation.AddColumn(new ColumnDef { Name = "id2", DataType = DataType.Integer.Instance, Nullable = true })]
            )
        );

        Assert.Throws<PlannerException>(
            () =>
                catalog.AlterTable(
                    new AlterTableStatement("users", [new AlterTableOperation.RenameColumn("id", "id2")])
                )
        );

        Assert.Throws<PlannerException>(
            () =>
                catalog.AlterTable(
                    new AlterTableStatement("users", [new AlterTableOperation.RenameTable("users")])
                )
        );

        var unknownKindPath = Path.Join(
            Path.GetTempPath(),
            $"aeternum-catalog-unknown-kind-{Guid.NewGuid():N}.json"
        );
        try
        {
            const string unknownKindState = """
                {
                  "Tables": [],
                  "Types": [
                    {
                      "Name": "mystery",
                      "Kind": "unknown_kind",
                      "Flag": false,
                      "Variants": [],
                      "Fields": []
                    }
                  ],
                  "Indexes": [],
                  "NextObjectId": 1
                }
                """;
            File.WriteAllText(unknownKindPath, unknownKindState);
            var unknownKindCatalog = new Catalog(unknownKindPath);
            Assert.True(unknownKindCatalog.TypeExists("mystery"));
        }
        finally
        {
            if (File.Exists(unknownKindPath))
                File.Delete(unknownKindPath);
            if (File.Exists($"{unknownKindPath}.tmp"))
                File.Delete($"{unknownKindPath}.tmp");
        }

        var nullKindPath = Path.Join(
            Path.GetTempPath(),
            $"aeternum-catalog-null-kind-{Guid.NewGuid():N}.json"
        );
        try
        {
            var nullKindCatalog = new Catalog(nullKindPath);
            nullKindCatalog.CreateTable(
                new TableSchema("n", [new ColumnSchema("id", DataType.BigInt.Instance, nullable: false)])
            );
            nullKindCatalog.AddType(new UserTypeSchema("null_kind_type", null!));
            nullKindCatalog.AddIndex(new IndexSchema("null_kind_idx", "n", ["id"], unique: false, null!));
            nullKindCatalog.Save();

            var reloaded = new Catalog(nullKindPath);
            Assert.True(reloaded.TypeExists("null_kind_type"));
            Assert.Contains(reloaded.GetTableIndexes("n"), i => i.Name == "null_kind_idx");
        }
        finally
        {
            if (File.Exists(nullKindPath))
                File.Delete(nullKindPath);
            if (File.Exists($"{nullKindPath}.tmp"))
                File.Delete($"{nullKindPath}.tmp");
        }
    }

    [Fact]
    public void SqlValidator_RemainingBranches_AreCovered()
    {
        var catalog = new Catalog();
        catalog.CreateTable(
            new TableSchema(
                "users",
                [
                    new ColumnSchema("id", DataType.BigInt.Instance, nullable: false),
                    new ColumnSchema("name", new DataType.Varchar(64), nullable: false),
                ]
            )
        );
        catalog.AddType(
            new UserTypeSchema("status_ok", new UserTypeKind.Enum(false, [new EnumVariant("OPEN")], [1]))
        );
        var validator = new SqlValidator(catalog);

        validator.Validate(
            new Statement.CreateEnum(
                new CreateEnumStatement("status_ok", flag: false, variants: [new EnumVariant("OPEN")], ifNotExists: true)
            )
        );
        validator.Validate(new Statement.DropEnum(new DropEnumStatement("status_ok", ifExists: true)));
        validator.Validate(new Statement.CreateDatabase(new CreateDatabaseStatement("db", ifNotExists: true)));

        Assert.Throws<ValidationException.ConstraintViolationException>(
            () =>
                validator.Validate(
                    new Statement.Insert(
                        new InsertStatement(
                            "users",
                            ["id", "name"],
                            [[new Expr.Literal(new SqlValue.Integer(1))]]
                        )
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
                            [[new Expr.Literal(new SqlValue.Integer(1))]]
                        )
                    )
                )
        );

        validator.Validate(
            new Statement.Select(
                new SelectStatement
                {
                    Columns =
                    [
                        new SelectItem.ExprItem(
                            new Expr.ArrayOp(
                                new Expr.Column(null, "id"),
                                BinaryOperator.Eq,
                                ArrayQuantifier.All,
                                new Expr.Literal(new SqlValue.Integer(1))
                            ),
                            "arr"
                        ),
                    ],
                    From = new TableReference.Named(null, null, "users", alias: null),
                    WhereClause = new Expr.InList(
                        new Expr.Column(null, "id"),
                        [new Expr.Literal(new SqlValue.Integer(1)), new Expr.Subquery(new SelectStatement { Columns = [new SelectItem.ExprItem(new Expr.Literal(new SqlValue.Integer(1)), null)] })],
                        negated: false
                    ),
                }
            )
        );

        validator.ValidateSequence(
            [
                new Statement.BeginTransaction(new BeginTransactionStatement { Name = "tx3" }),
                new Statement.Rollback(new RollbackStatement(RollbackScope.All.Instance, chain: false)),
            ]
        );
    }

    [Fact]
    public void CatalogPersistedDataTypeJsonCodec_CoalesceBranches_AreCovered()
    {
        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"reference","DataId":"fallback_users"}""",
                out var fallbackReference
            )
        );
        Assert.Equal("fallback_users", Assert.IsType<DataType.Reference>(fallbackReference).Table);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"reference_array","DataId":"fallback_users"}""",
                out var fallbackReferenceArray
            )
        );
        Assert.Equal("fallback_users", Assert.IsType<DataType.ReferenceArray>(fallbackReferenceArray).Table);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"virtual_reference"}""",
                out var fallbackVirtualReference
            )
        );
        var vr = Assert.IsType<DataType.VirtualReference>(fallbackVirtualReference);
        Assert.Equal("UNKNOWN", vr.Table);
        Assert.Equal("UNKNOWN", vr.Column);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"virtual_reference_array"}""",
                out var fallbackVirtualReferenceArray
            )
        );
        var vra = Assert.IsType<DataType.VirtualReferenceArray>(fallbackVirtualReferenceArray);
        Assert.Equal("UNKNOWN", vra.Table);
        Assert.Equal("UNKNOWN", vra.Column);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"user_defined","DataId":"fallback_type"}""",
                out var fallbackEnumRef
            )
        );
        Assert.Equal("fallback_type", Assert.IsType<DataType.EnumRef>(fallbackEnumRef).Name);

        Assert.True(
            CatalogPersistedDataTypeJsonCodec.TryParseDataType(
                """{"Kind":"array"}""",
                out var unknownArray
            )
        );
        Assert.Equal("UNKNOWN", Assert.IsType<DataType.Vector>(unknownArray).ElementType.ToString());
    }

    [Fact]
    public void Catalog_LoadWithWhitespacePath_AndDropColumnBranches_AreCovered()
    {
        var noPathCatalog = new Catalog(" ");
        noPathCatalog.Save();
        noPathCatalog.Load();

        var catalog = new Catalog();
        catalog.CreateTable(
            new TableSchema(
                "users",
                [
                    new ColumnSchema("id", DataType.BigInt.Instance, nullable: false),
                    new ColumnSchema("name", new DataType.Varchar(64), nullable: true),
                ]
            )
        );

        catalog.AlterTable(
            new AlterTableStatement("users", [new AlterTableOperation.DropColumn("name", ifExists: false)])
        );
        catalog.AlterTable(
            new AlterTableStatement("users", [new AlterTableOperation.DropColumn("missing", ifExists: true)])
        );

        Assert.NotNull(catalog.GetTable("users")!.GetColumn("id"));
        Assert.Null(catalog.GetTable("users")!.GetColumn("name"));
    }

    [Fact]
    public void SqlValidator_ViewAsAndWalkExpr_RemainingBranches_AreCovered()
    {
        var catalog = new Catalog();
        catalog.CreateTable(
            new TableSchema(
                "users",
                [
                    new ColumnSchema("id", DataType.BigInt.Instance, nullable: false),
                    new ColumnSchema("name", new DataType.Varchar(64), nullable: false),
                ]
            )
        );

        var validator = new SqlValidator(catalog);

        validator.Validate(
            new Statement.AlterTable(
                new AlterTableStatement(
                    "users",
                    [new AlterTableOperation.RenameColumn("name", "display_name")]
                )
            )
        );

        validator.Validate(
            new Statement.Insert(
                new InsertStatement("users", [], [])
            )
        );

        validator.Validate(
            new Statement.Select(
                new SelectStatement
                {
                    Columns = [new SelectItem.ExprItem(new Expr.Column(null, "id"), "id")],
                    From = new TableReference.Named(null, null, "users", alias: null),
                    ViewAs =
                    [
                        new ViewAsItem(
                            new Expr.Case(
                                operand: new Expr.UnaryOp(
                                    UnaryOperator.Not,
                                    new Expr.IsNull(new Expr.Column(null, "display_name"), negated: false)
                                ),
                                conditions:
                                [
                                    (
                                        new Expr.InList(
                                            new Expr.Column(null, "display_name"),
                                            [new Expr.Literal(new SqlValue.SqlString("a"))],
                                            negated: false
                                        ),
                                        new Expr.Cast(
                                            new Expr.Substring(
                                                new Expr.Column(null, "display_name"),
                                                new Expr.Literal(new SqlValue.Integer(1)),
                                                null
                                            ),
                                            new DataType.Varchar(10)
                                        )
                                    ),
                                ],
                                elseResult: new Expr.Literal(new SqlValue.SqlString("fallback"))
                            ),
                            "normalized_name"
                        ),
                    ],
                }
            )
        );

        validator.Validate(
            new Statement.Select(
                new SelectStatement
                {
                    Columns =
                    [
                        new SelectItem.ExprItem(
                            new Expr.Case(
                                operand: new Expr.Column(null, "display_name"),
                                conditions:
                                [
                                    (
                                        new Expr.UnaryOp(
                                            UnaryOperator.Not,
                                            new Expr.IsNull(new Expr.Column(null, "display_name"), negated: false)
                                        ),
                                        new Expr.Substring(
                                            new Expr.Column(null, "display_name"),
                                            new Expr.Literal(new SqlValue.Integer(1)),
                                            new Expr.Literal(new SqlValue.Integer(2))
                                        )
                                    ),
                                ],
                                elseResult: new Expr.Literal(new SqlValue.SqlString("fallback"))
                            ),
                            "case_name"
                        ),
                    ],
                    From = new TableReference.Join(
                        new TableReference.Named(null, null, "users", "u"),
                        new TableReference.Named(null, null, "users", "u2"),
                        JoinType.Inner,
                        new Expr.BinaryOp(
                            new Expr.Column("u", "id"),
                            BinaryOperator.Eq,
                            new Expr.Column("u2", "id")
                        )
                    ),
                    WhereClause = new Expr.Function("LOWER", [new Expr.Column("u", "display_name")], false),
                }
            )
        );

        validator.Validate(
            new Statement.AlterTable(
                new AlterTableStatement(
                    "users",
                    [new AlterTableOperation.AddColumn(new ColumnDef { Name = "extra", DataType = DataType.Integer.Instance, Nullable = true })]
                )
            )
        );

        Assert.NotNull(catalog.GetTable("users")!.GetColumn("name"));
    }

    [Fact]
    public void SqlValidator_TransactionNamedSuccessBranches_AreCovered()
    {
        var validator = new SqlValidator(new Catalog());
        var commitException = Record.Exception(
            () =>
                validator.ValidateSequence(
                    [
                        new Statement.BeginTransaction(new BeginTransactionStatement { Name = "root" }),
                        new Statement.Commit(new CommitStatement(new CommitScope.Named("root"), chain: false)),
                    ]
                )
        );
        Assert.Null(commitException);

        var rollbackException = Record.Exception(
            () =>
                validator.ValidateSequence(
                    [
                        new Statement.BeginTransaction(new BeginTransactionStatement { Name = "root2" }),
                        new Statement.Rollback(
                            new RollbackStatement(new RollbackScope.Named("root2"), chain: false)
                        ),
                    ]
                )
        );
        Assert.Null(rollbackException);
    }
}
