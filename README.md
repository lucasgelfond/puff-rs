# puff-rs

A modern Rust client for [Turbopuffer](https://turbopuffer.com), the serverless vector database.

## Development

This is a bit of an experiment in agent-based coding. I have a little Rust-based Turbopuffer project I'd started on, but the only existing client I see, the unofficial [ragkit/turbopuffer-client](https://github.com/ragkit/turbopuffer-client) looks unmaintained and untyped. All of the existing clients are open source, making this a pretty agent-tractable problem. 

For dev setup, I had Claude write an AGENTS.md that specified where the docs are, and I manually cloned all of the other clients to `other-clients/`, which I .gitignore-d. Then I just had Claude Opus 4.5 rip it with those instructions. 

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
puff-rs = "0.1"
```

## Quick Start

```rust
use puff_rs::{Client, DistanceMetric, Filter, QueryParams, RankBy, WriteParams};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client from environment (reads TURBOPUFFER_API_KEY)
    let client = Client::from_env()?;

    // Get a namespace
    let ns = client.namespace("my-namespace");

    // Write documents
    let mut row = HashMap::new();
    row.insert("id".to_string(), serde_json::json!(1));
    row.insert("vector".to_string(), serde_json::json!([0.1, 0.2, 0.3, 0.4]));
    row.insert("name".to_string(), serde_json::json!("alice"));

    ns.write(WriteParams {
        upsert_rows: Some(vec![row]),
        distance_metric: Some(DistanceMetric::CosineDistance),
        ..Default::default()
    }).await?;

    // Query with vector search
    let results = ns.query(QueryParams {
        rank_by: Some(RankBy::vector("vector", vec![0.1, 0.2, 0.3, 0.4])),
        top_k: Some(10),
        filters: Some(Filter::eq("name", "alice")),
        ..Default::default()
    }).await?;

    for row in results.rows {
        println!("{:?}", row);
    }

    Ok(())
}
```

## Client Configuration

```rust
// From environment variable TURBOPUFFER_API_KEY
let client = Client::from_env()?;

// With explicit API key
let client = Client::new("your-api-key");

// With specific region
let client = Client::with_region("your-api-key", "gcp-us-east1");

// With custom base URL
let client = Client::with_base_url("your-api-key", "https://custom.endpoint.com");
```

## Namespace Operations

```rust
let ns = client.namespace("my-namespace");

// Write/upsert documents
ns.write(WriteParams { ... }).await?;

// Query documents
ns.query(QueryParams { ... }).await?;

// Multi-query (batch multiple queries)
ns.multi_query(MultiQueryParams { ... }).await?;

// Delete all documents
ns.delete_all().await?;

// Get namespace metadata
ns.metadata().await?;

// Get schema
ns.schema().await?;

// Hint cache warm
ns.hint_cache_warm().await?;
```

## Filters

Filters use a tuple-based format that matches the Turbopuffer API:

```rust
use puff_rs::Filter;

// Comparison operators
Filter::eq("name", "alice")           // ["name", "Eq", "alice"]
Filter::not_eq("status", "deleted")   // ["status", "NotEq", "deleted"]
Filter::lt("age", 30)                 // ["age", "Lt", 30]
Filter::lte("age", 30)                // ["age", "Lte", 30]
Filter::gt("score", 0.5)              // ["score", "Gt", 0.5]
Filter::gte("score", 0.5)             // ["score", "Gte", 0.5]

// Set operators
Filter::r#in("status", vec!["active".into(), "pending".into()])
Filter::not_in("status", vec!["deleted".into()])
Filter::contains("tags", "rust")
Filter::contains_any("tags", vec!["rust".into(), "python".into()])

// String operators
Filter::glob("name", "a*")            // Glob pattern matching
Filter::iglob("name", "A*")           // Case-insensitive glob
Filter::regex("email", r".*@.*\.com") // Regex matching

// Logical operators
Filter::and(vec![
    Filter::gte("age", 18),
    Filter::eq("status", "active"),
])
Filter::or(vec![
    Filter::eq("role", "admin"),
    Filter::eq("role", "moderator"),
])
Filter::not(Filter::eq("deleted", true))
```

## Ranking

```rust
use puff_rs::RankBy;

// Vector similarity (ANN)
RankBy::vector("vector", vec![0.1, 0.2, 0.3, 0.4])

// Exact k-NN
RankBy::vector_knn("vector", vec![0.1, 0.2, 0.3, 0.4])

// BM25 text search
RankBy::bm25("content", "search query")

// Attribute ordering
RankBy::asc("timestamp")
RankBy::desc("score")

// Combine rankings
RankBy::sum(vec![
    RankBy::bm25("title", "query"),
    RankBy::bm25("content", "query"),
])
RankBy::product(2.0, RankBy::bm25("title", "query"))
```

## Distance Metrics

```rust
use puff_rs::DistanceMetric;

DistanceMetric::CosineDistance
DistanceMetric::EuclideanSquared
```

## Listing Namespaces

```rust
use puff_rs::NamespacesParams;

let response = client.namespaces(NamespacesParams {
    prefix: Some("prod-".to_string()),
    page_size: Some(100),
    cursor: None,
}).await?;

for ns in response.namespaces {
    println!("{}", ns.id);
}
```

## Environment Variables

- `TURBOPUFFER_API_KEY` - Your Turbopuffer API key (required for `Client::from_env()`)
- `TURBOPUFFER_REGION` - Optional region (e.g., `gcp-us-east1`)

## License

MIT
