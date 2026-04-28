# PR 4.1: SQL Grammar Extensions — `FILTER BY` & `FLAT TABLE` Native Keywords

## 📋 Overview

**PR Number:** 4.1
**Phase:** 4 — SQL Grammar & Language Extensions
**Priority:** 🟡 High
**Estimated Effort:** 5 days
**Dependencies:** PR 1.3 (SQL Parser), PR 1.4 (Query Planner), PR 1.5 (Query Executor)

## 🎯 Objectives

Promote AeternumDB-specific SQL constructs from parser-level workarounds to
first-class keywords recognised by the `AeternumDialect` grammar:

1. **`FILTER BY`** — replace the `ON` clause in join traversal with a native keyword that
   semantically binds an extra filter predicate to a reference-column join, without implying
   a traditional relational join.

2. **`CREATE FLAT TABLE`** — native keyword modifier that marks a table as `FLAT` directly
   in the grammar, so `CREATE FLAT TABLE name (…)` is parsed as a valid SQL statement
   rather than being handled only via a post-parse flag.

These keywords are currently scaffolded as AST flags (`filter_by: Option<Expr>`,
`CreateTableStatement::flat: bool`) and accepted via SQL surface workarounds. This PR
adds them to the `AeternumDialect` grammar so they are first-class syntax.

## 📝 Detailed Prompt for Implementation

```
Extend AeternumDialect in core/src/sql/dialect.rs with the following grammar additions:

1. **`FILTER BY` clause**
   - Grammar: FROM table [FILTER BY <expr>]
   - Replace existing workaround that maps ON → filter_by
   - Parser: after consuming the chain reference, look for the keyword FILTER followed
     by BY and consume the predicate expression
   - AST: TableReference::Join already has `filter_by: Option<Expr>` — no AST change needed
   - Error: if both FILTER BY and ON are present, return a parse error:
     "Use FILTER BY instead of ON for reference-column joins"
   - Validator: FILTER BY is not allowed on FLAT tables (they have no joins)

2. **`FLAT` modifier on CREATE TABLE**
   - Grammar: CREATE [FLAT] TABLE name (columns…) [options]
   - The FLAT keyword must appear between CREATE and TABLE
   - AST: CreateTableStatement::flat already exists — no AST change needed
   - Validator: FLAT tables cannot carry VERSIONED flag (error if combined)
   - Validator: FLAT tables cannot have reference or vector-reference columns
   - Error messages must be explicit and actionable

3. **Keyword Registration**
   - Add FILTER, BY, FLAT to the list of reserved keywords in AeternumDialect
   - Ensure the keywords are not usable as unquoted column or table names
   - Provide a helpful error if a user tries to use them as identifiers:
     "FILTER/FLAT is a reserved keyword; quote it with backticks or double-quotes"

4. **Backward Compatibility**
   - Existing SQL that uses ON for join predicates must continue to parse but
     emit a deprecation warning (not an error) pointing to FILTER BY
   - Tests must cover both syntaxes

5. **Documentation**
   - Update docs/sql-reference.md to show FILTER BY as the canonical syntax
   - Update docs/sql-reference.md to show CREATE FLAT TABLE
   - Remove any "workaround" notes from the documentation
```

## 📁 Files to Create / Modify

```
core/src/sql/dialect.rs     — add FILTER, BY, FLAT as reserved keywords;
                               override keyword_sets() or is_reserved_for_column_alias()
core/src/sql/ast.rs         — (no structural change expected; may add deprecation warning)
core/src/sql/parser.rs      — handle FILTER BY in the SELECT / FROM parsing path
core/src/sql/validator.rs   — enforce FLAT+VERSIONED mutual exclusion;
                               enforce FLAT+reference-column mutual exclusion
core/tests/sql_parser_tests.rs — new tests for FILTER BY and CREATE FLAT TABLE
docs/sql-reference.md       — promote FILTER BY and CREATE FLAT TABLE as canonical
docs/prs/PR-4.1-sql-grammar-extensions.md — this file
```

## ✅ Tests Required

- [ ] `test_filter_by_parsed` — `SELECT … FILTER BY col = 1` parses to `filter_by: Some(…)`
- [ ] `test_on_deprecated_warning` — `SELECT … ON col = 1` parses and emits a warning
- [ ] `test_filter_by_flat_rejected` — `SELECT … FILTER BY` on a FLAT table fails validation
- [ ] `test_create_flat_table_parsed` — `CREATE FLAT TABLE t (…)` parses with `flat: true`
- [ ] `test_create_table_flat_versioned_rejected` — `CREATE FLAT VERSIONED TABLE` fails
- [ ] `test_create_flat_table_with_ref_col_rejected` — reference column in FLAT table fails
- [ ] `test_flat_keyword_as_identifier_rejected` — `CREATE TABLE flat (…)` fails with helpful error
- [ ] `test_filter_keyword_as_identifier_rejected` — `SELECT filter FROM t` fails with helpful error

## 📊 Performance Targets

No performance regressions on the parser benchmark suite:

| Metric | Target |
|--------|--------|
| Simple SELECT parse time | < 1 ms |
| Complex SELECT with FILTER BY | < 2 ms |
| CREATE FLAT TABLE parse time | < 1 ms |

## 📚 Documentation Requirements

- `docs/sql-reference.md` — update all join examples to use `FILTER BY`; update all FLAT table
  examples to use `CREATE FLAT TABLE`; remove workaround notes
- `docs/prs/PR-4.1-sql-grammar-extensions.md` — this file

## ✔️ Acceptance Criteria

- [ ] `FILTER BY` is parsed as a first-class join predicate keyword in `AeternumDialect`
- [ ] `CREATE FLAT TABLE` is parsed without requiring any workaround
- [ ] `FILTER BY` on a FLAT table is rejected with a clear error
- [ ] `FLAT` + `VERSIONED` combination is rejected with a clear error
- [ ] FLAT table with a reference column is rejected with a clear error
- [ ] `FILTER` and `FLAT` are reserved keywords; using them as unquoted identifiers fails
- [ ] All existing tests continue to pass
- [ ] All new tests pass; no clippy warnings; cargo fmt clean
- [ ] `docs/sql-reference.md` updated to use canonical syntax throughout

## 🔗 What Comes Next

| Work Item | Target PR |
|-----------|-----------|
| `EXPAND(ref_col)` as a native grammar keyword | PR 4.2 |
| `VIEW AS` as a native grammar clause | PR 4.2 |
| `CREATE ENUM` / `DROP ENUM` as native grammar | PR 4.3 |
| Full AeternumDB SQL dialect formalisation (BNF grammar doc) | PR 4.4 |
