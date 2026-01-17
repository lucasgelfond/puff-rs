# puff-rs Agent Guide

Build a feature-complete, tested, rigorously typed Rust client for Turbopuffer that mirrors the official SDKs.

## Reference

- **API Docs**: https://turbopuffer.com/docs
- **Existing clients**: `other-clients/` (Go, TypeScript, Python, Ruby, Java)
- The official clients are generated from an OpenAPI spec via Stainless - study them for exact API shapes

## Project Structure

Mirror the official clients with idiomatic Rust. Use small, focused files:

```
src/
  lib.rs                    # Re-exports public API
  client.rs                 # Client struct, config, base HTTP
  namespace.rs              # Namespace resource and operations
  error.rs                  # Error types (keep minimal)

  types/
    mod.rs                  # Re-exports all types
    id.rs                   # Id enum (u64 | String)
    distance_metric.rs      # DistanceMetric enum
    vector.rs               # Vector (f32 array or base64 string)
    vector_encoding.rs      # VectorEncoding enum
    row.rs                  # Row type alias (HashMap)
    columns.rs              # Columns type (columnar format)
    language.rs             # Language enum (for FTS)
    tokenizer.rs            # Tokenizer enum (for FTS)

  filter/
    mod.rs                  # Filter enum + constructors + re-exports
    # Filter serializes as JSON arrays: ["attr", "Op", value]
    # Logical ops: ["And", [...]], ["Or", [...]], ["Not", filter]

  rank_by/
    mod.rs                  # RankBy enum + constructors + re-exports
    # Vector: ["attr", "ANN", [f32...]] or ["attr", "kNN", [f32...]]
    # BM25: ["attr", "BM25", "query"]
    # Attribute: ["attr", "asc"|"desc"]
    # Combinators: ["Sum", [...]], ["Max", [...]], ["Product", weight, subquery]

  aggregate/
    mod.rs                  # AggregateBy enum
    # Count: ["Count"]
    # Sum: ["Sum", "attr"]

  schema/
    mod.rs                  # AttributeSchemaConfig, FullTextSearchConfig

  params/
    mod.rs                  # Re-exports
    write.rs                # NamespaceWriteParams
    query.rs                # NamespaceQueryParams
    multi_query.rs          # NamespaceMultiQueryParams

  responses/
    mod.rs                  # Re-exports
    write.rs                # NamespaceWriteResponse, WriteBilling
    query.rs                # NamespaceQueryResponse, QueryBilling, QueryPerformance
    metadata.rs             # NamespaceMetadata
    schema.rs               # NamespaceSchemaResponse

tests/
  client_test.rs            # Client construction tests
  namespace_test.rs         # Namespace operation tests (against mock or real API)
  filter_test.rs            # Filter serialization tests
  rank_by_test.rs           # RankBy serialization tests
  custom_test.rs            # Integration tests

examples/
  write_and_query.rs        # Basic upsert + query example
  full_text_search.rs       # BM25 example
  filters.rs                # Complex filter example
```

## API Surface

### Client

```rust
let client = Client::new("tpuf_api_key");
// or with options
let client = Client::builder()
    .api_key("tpuf_api_key")
    .region("gcp-us-central1")  // sets base URL
    .base_url("https://custom.url")  // override
    .build();

// Environment variable support
let client = Client::from_env();  // reads TURBOPUFFER_API_KEY, TURBOPUFFER_REGION
```

### Namespace Operations

```rust
let ns = client.namespace("my-namespace");

// Write (POST /v2/namespaces/:ns)
let response = ns.write(WriteParams {
    upsert_rows: Some(vec![
        row!{"id" => 1, "vector" => vec![0.1, 0.2], "name" => "foo"},
    ]),
    distance_metric: Some(DistanceMetric::CosineDistance),
    ..Default::default()
}).await?;

// Query (POST /v2/namespaces/:ns/query)
let response = ns.query(QueryParams {
    rank_by: Some(RankBy::vector("vector", vec![0.1, 0.2])),
    top_k: Some(10),
    filters: Some(Filter::eq("name", "foo")),
    include_attributes: Some(IncludeAttributes::List(vec!["name".into()])),
    ..Default::default()
}).await?;

// Other operations
ns.delete_all().await?;
ns.metadata().await?;
ns.schema().await?;
ns.update_schema(schema).await?;
ns.hint_cache_warm().await?;
ns.multi_query(queries).await?;
ns.explain_query(params).await?;
ns.recall(params).await?;
```

### Client-Level Operations

```rust
// List namespaces (GET /v1/namespaces)
let page = client.namespaces(NamespacesParams {
    prefix: Some("test-".into()),
    page_size: Some(100),
    ..Default::default()
}).await?;

// Auto-pagination
let mut pager = client.namespaces_auto_paging(params);
while let Some(ns) = pager.next().await? {
    println!("{}", ns.id);
}
```

## Key Types

### Filter

Filters serialize as JSON arrays. Use constructor functions:

```rust
pub enum Filter {
    // Comparison: ["attr", "Op", value]
    Eq { attr: String, value: Value },
    NotEq { attr: String, value: Value },
    Lt { attr: String, value: Value },
    Lte { attr: String, value: Value },
    Gt { attr: String, value: Value },
    Gte { attr: String, value: Value },

    // Array ops: ["attr", "Op", [values]]
    In { attr: String, values: Vec<Value> },
    NotIn { attr: String, values: Vec<Value> },
    Contains { attr: String, value: Value },
    ContainsAny { attr: String, values: Vec<Value> },

    // String ops
    Glob { attr: String, pattern: String },
    IGlob { attr: String, pattern: String },
    Regex { attr: String, pattern: String },

    // FTS ops
    ContainsAllTokens { attr: String, query: String, params: Option<ContainsAllTokensParams> },
    ContainsTokenSequence { attr: String, query: String },

    // Logical: ["And"|"Or", [filters]] or ["Not", filter]
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

// Constructors
impl Filter {
    pub fn eq(attr: impl Into<String>, value: impl Into<Value>) -> Self;
    pub fn and(filters: Vec<Filter>) -> Self;
    // etc.
}
```

### RankBy

```rust
pub enum RankBy {
    // ["attr", "ANN", [vector]]
    Vector { attr: String, query: Vec<f32> },
    // ["attr", "BM25", "query"]
    Bm25 { attr: String, query: String, params: Option<Bm25Params> },
    // ["attr", "asc"|"desc"]
    Attribute { attr: String, order: Order },
    // Combinators
    Sum(Vec<RankBy>),
    Max(Vec<RankBy>),
    Product { weight: f64, subquery: Box<RankBy> },
}
```

### Id

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Id {
    Uint(u64),
    String(String),
}
```

## Serialization

The tricky part is that Filter/RankBy serialize as JSON arrays, not objects:

```rust
// Filter::Eq { attr: "name", value: "foo" } -> ["name", "Eq", "foo"]
// Filter::And([f1, f2]) -> ["And", [f1_json, f2_json]]
// RankBy::Vector { attr: "v", query: [0.1] } -> ["v", "ANN", [0.1]]

impl Serialize for Filter {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Filter::Eq { attr, value } => (attr, "Eq", value).serialize(s),
            Filter::And(filters) => ("And", filters).serialize(s),
            // etc.
        }
    }
}
```

## Testing

1. **Unit tests**: Filter/RankBy serialization, type conversions
2. **Integration tests**: Against real API (use test namespace prefix, clean up)
3. **Mock tests**: Against Prism mock server (like other clients do)

Test patterns from Go client:
```rust
#[tokio::test]
async fn test_write_and_query() {
    let client = test_client();
    let ns = client.namespace(&test_namespace("write-query"));

    // Write
    ns.write(WriteParams {
        upsert_rows: Some(vec![...]),
        distance_metric: Some(DistanceMetric::CosineDistance),
        ..Default::default()
    }).await.unwrap();

    // Query
    let res = ns.query(QueryParams {
        rank_by: Some(RankBy::vector("vector", vec![0.1, 0.2])),
        top_k: Some(10),
        ..Default::default()
    }).await.unwrap();

    assert!(!res.rows.is_empty());

    // Cleanup
    ns.delete_all().await.unwrap();
}
```

## Dependencies

Keep minimal:
- `serde`, `serde_json` - serialization
- `reqwest` - HTTP (with `json` feature)
- `thiserror` - error types
- `tokio` - async runtime (dev-dependency for tests)

## Implementation Order

1. **Foundation**
   - [ ] `error.rs` - basic error types
   - [ ] `client.rs` - Client struct, HTTP plumbing
   - [ ] `namespace.rs` - Namespace struct (no methods yet)

2. **Core Types**
   - [ ] `types/id.rs`
   - [ ] `types/distance_metric.rs`
   - [ ] `types/row.rs`

3. **Write Operation**
   - [ ] `params/write.rs` - WriteParams
   - [ ] `responses/write.rs` - WriteResponse
   - [ ] `namespace.rs` - add write() method
   - [ ] Test: upsert rows, verify response

4. **Query Operation**
   - [ ] `filter/mod.rs` - Filter enum with serialization
   - [ ] `rank_by/mod.rs` - RankBy enum with serialization
   - [ ] `params/query.rs` - QueryParams
   - [ ] `responses/query.rs` - QueryResponse
   - [ ] `namespace.rs` - add query() method
   - [ ] Test: query with filters, verify rows

5. **Remaining Operations**
   - [ ] delete_all, metadata, schema, update_schema
   - [ ] hint_cache_warm, multi_query, explain_query, recall
   - [ ] Client.namespaces() with pagination

6. **Polish**
   - [ ] Builder pattern for Client
   - [ ] Environment variable support
   - [ ] Examples
   - [ ] Documentation

## Style Guidelines

- Minimal code, no unnecessary abstractions
- Only add comments where logic is non-obvious
- Derive common traits: `Debug, Clone, Serialize, Deserialize`
- Use `#[serde(skip_serializing_if = "Option::is_none")]` for optional fields
- Prefer enums over stringly-typed values
- Test serialization output matches API expectations
