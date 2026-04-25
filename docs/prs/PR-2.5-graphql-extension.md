# PR 2.5: GraphQL Engine Extension (Example)

## 📋 Overview

**PR Number:** 2.5  
**Phase:** 2 - Extensibility  
**Priority:** 🟢 Medium  
**Estimated Effort:** 7 days  
**Dependencies:** PR 2.4 (Hello World Extension)

## 🎯 Objectives

Implement GraphQL query language extension as complex example demonstrating schema introspection, query parsing, and execution via SQL translation.

## 📝 Detailed Prompt

```
Create GraphQL extension with:
1. GraphQL query parsing using graphql-parser
2. Schema definition and introspection
3. Query execution via SQL translation
4. Type mapping between GraphQL and SQL
5. Resolver implementation
6. Mutation support
7. Subscription support (basic)
8. Error handling
```

## 🏗️ Files to Create

1. `extensions/graphql/Cargo.toml`
2. `extensions/graphql/src/lib.rs`
3. `extensions/graphql/src/parser.rs`
4. `extensions/graphql/src/executor.rs`
5. `extensions/graphql/src/schema.rs`
6. `extensions/graphql/src/resolver.rs`
7. `extensions/graphql/tests/`

## 🔧 Implementation

### GraphQL Schema
```graphql
type User {
  id: ID!
  name: String!
  email: String
  posts: [Post!]!
}

type Post {
  id: ID!
  title: String!
  content: String
  author: User!
}

type Query {
  user(id: ID!): User
  users: [User!]!
  post(id: ID!): Post
  posts: [Post!]!
}
```

### Extension Code
```rust
use graphql_parser::query::*;

#[extension_function]
fn execute_graphql(query: &str) -> Result<String, Error> {
    // Parse GraphQL query
    let doc = parse_query::<&str>(query)?;
    
    // Translate to SQL
    let sql = translate_to_sql(&doc)?;
    
    // Execute query
    let result = execute_query(&sql)?;
    
    // Format as GraphQL response
    let response = format_graphql_response(result)?;
    
    Ok(response)
}

fn translate_to_sql(doc: &Document) -> Result<String, Error> {
    match &doc.definitions[0] {
        Definition::Operation(op) => {
            match op {
                OperationDefinition::Query(query) => {
                    translate_query(query)
                }
                _ => Err(Error::Unsupported("Only queries supported"))
            }
        }
        _ => Err(Error::InvalidQuery)
    }
}

fn translate_query(query: &Query) -> Result<String, Error> {
    let selection_set = &query.selection_set;
    
    // Simple example: { users { id name } }
    // Translates to: SELECT id, name FROM users
    
    let mut sql = String::from("SELECT ");
    
    // Extract fields
    for selection in &selection_set.items {
        if let Selection::Field(field) = selection {
            sql.push_str(&field.name);
            sql.push_str(", ");
        }
    }
    sql.truncate(sql.len() - 2); // Remove trailing comma
    
    sql.push_str(" FROM ");
    // Add table name based on query
    
    Ok(sql)
}
```

## ✅ Tests Required

- [ ] Parse GraphQL queries
- [ ] Schema introspection
- [ ] Execute queries
- [ ] Type mapping
- [ ] Nested queries
- [ ] Mutations
- [ ] Error handling

## 🚀 Implementation Steps

**Day 1-2:** GraphQL parsing  
**Day 3-4:** SQL translation  
**Day 5:** Resolver implementation  
**Day 6:** Mutations and testing  
**Day 7:** Documentation

---

**Ready to implement!** 🚀
