use puff_rs::{Client, DistanceMetric, Filter, QueryParams, RankBy, WriteParams};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client from environment
    let client = Client::from_env()?;

    // Get a namespace
    let ns = client.namespace("rust-example");

    // Write some documents
    let mut row1 = HashMap::new();
    row1.insert("id".to_string(), serde_json::json!(1));
    row1.insert("vector".to_string(), serde_json::json!([0.1, 0.2, 0.3, 0.4]));
    row1.insert("name".to_string(), serde_json::json!("alice"));
    row1.insert("age".to_string(), serde_json::json!(30));

    let mut row2 = HashMap::new();
    row2.insert("id".to_string(), serde_json::json!(2));
    row2.insert("vector".to_string(), serde_json::json!([0.2, 0.3, 0.4, 0.5]));
    row2.insert("name".to_string(), serde_json::json!("bob"));
    row2.insert("age".to_string(), serde_json::json!(25));

    let write_response = ns
        .write(WriteParams {
            upsert_rows: Some(vec![row1, row2]),
            distance_metric: Some(DistanceMetric::CosineDistance),
            ..Default::default()
        })
        .await?;

    println!("Wrote {} rows", write_response.rows_affected);

    // Query with vector search
    let query_response = ns
        .query(QueryParams {
            rank_by: Some(RankBy::vector("vector", vec![0.15, 0.25, 0.35, 0.45])),
            top_k: Some(10),
            filters: Some(Filter::gte("age", 20)),
            ..Default::default()
        })
        .await?;

    println!("Found {} results:", query_response.rows.len());
    for row in &query_response.rows {
        println!("  {:?}", row);
    }

    // Query with attribute ordering
    let ordered = ns
        .query(QueryParams {
            rank_by: Some(RankBy::desc("age")),
            top_k: Some(10),
            ..Default::default()
        })
        .await?;

    println!("\nOrdered by age desc:");
    for row in &ordered.rows {
        println!("  {:?}", row);
    }

    // Clean up
    ns.delete_all().await?;
    println!("\nDeleted namespace");

    Ok(())
}
