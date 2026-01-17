use serde::Deserialize;
use std::collections::HashMap;

use crate::Row;

#[derive(Debug, Clone, Deserialize)]
pub struct WriteResponse {
    pub rows_affected: u64,

    #[serde(default)]
    pub rows_upserted: Option<u64>,

    #[serde(default)]
    pub rows_patched: Option<u64>,

    #[serde(default)]
    pub rows_deleted: Option<u64>,

    #[serde(default)]
    pub rows_remaining: Option<bool>,

    #[serde(default)]
    pub upserted_ids: Option<Vec<serde_json::Value>>,

    #[serde(default)]
    pub patched_ids: Option<Vec<serde_json::Value>>,

    #[serde(default)]
    pub deleted_ids: Option<Vec<serde_json::Value>>,

    #[serde(default)]
    pub billing: Option<WriteBilling>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WriteBilling {
    pub billable_logical_bytes_written: u64,

    #[serde(default)]
    pub query: Option<QueryBillingInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryBillingInfo {
    pub billable_logical_bytes_queried: u64,
    pub billable_logical_bytes_returned: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryResponse {
    #[serde(default)]
    pub rows: Vec<Row>,

    #[serde(default)]
    pub aggregations: Option<HashMap<String, serde_json::Value>>,

    #[serde(default)]
    pub aggregation_groups: Option<Vec<HashMap<String, serde_json::Value>>>,

    #[serde(default)]
    pub billing: Option<QueryBilling>,

    #[serde(default)]
    pub performance: Option<QueryPerformance>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryBilling {
    pub billable_logical_bytes_queried: u64,
    pub billable_logical_bytes_returned: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryPerformance {
    #[serde(default)]
    pub cache_hit_ratio: Option<f64>,

    #[serde(default)]
    pub cache_temperature: Option<String>,

    #[serde(default)]
    pub server_total_ms: Option<u64>,

    #[serde(default)]
    pub query_execution_ms: Option<u64>,

    #[serde(default)]
    pub exhaustive_search_count: Option<u64>,

    #[serde(default)]
    pub approx_namespace_size: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MultiQueryResponse {
    pub results: Vec<QueryResponse>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeleteAllResponse {
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NamespaceMetadata {
    #[serde(default)]
    pub approx_count: Option<u64>,

    #[serde(default)]
    pub dimensions: Option<u32>,

    #[serde(default)]
    pub created_at: Option<String>,

    #[serde(default)]
    pub unindexed_bytes: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaResponse(pub HashMap<String, serde_json::Value>);

#[derive(Debug, Clone, Deserialize)]
pub struct HintCacheWarmResponse {
    pub status: String,

    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NamespaceSummary {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NamespacesResponse {
    pub namespaces: Vec<NamespaceSummary>,

    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_response_deserialization() {
        let json = r#"{"rows_affected": 5}"#;
        let resp: WriteResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.rows_affected, 5);
        assert!(resp.rows_upserted.is_none());
    }

    #[test]
    fn test_write_response_full() {
        let json = r#"{
            "rows_affected": 10,
            "rows_upserted": 5,
            "rows_patched": 3,
            "rows_deleted": 2,
            "upserted_ids": [1, 2, 3, 4, 5],
            "billing": {
                "billable_logical_bytes_written": 1024
            }
        }"#;
        let resp: WriteResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.rows_affected, 10);
        assert_eq!(resp.rows_upserted, Some(5));
        assert_eq!(resp.rows_patched, Some(3));
        assert_eq!(resp.rows_deleted, Some(2));
        assert_eq!(resp.upserted_ids.as_ref().unwrap().len(), 5);
        assert!(resp.billing.is_some());
    }

    #[test]
    fn test_query_response_deserialization() {
        let json = r#"{
            "rows": [
                {"id": 1, "name": "alice", "_dist": 0.1},
                {"id": 2, "name": "bob", "_dist": 0.2}
            ]
        }"#;
        let resp: QueryResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.rows.len(), 2);
    }

    #[test]
    fn test_query_response_with_aggregations() {
        let json = r#"{
            "rows": [],
            "aggregations": {"count": 42, "avg_score": 0.75}
        }"#;
        let resp: QueryResponse = serde_json::from_str(json).unwrap();
        assert!(resp.aggregations.is_some());
        let aggs = resp.aggregations.unwrap();
        assert_eq!(aggs.get("count").unwrap(), 42);
    }

    #[test]
    fn test_query_response_with_performance() {
        let json = r#"{
            "rows": [],
            "performance": {
                "cache_hit_ratio": 0.95,
                "cache_temperature": "hot",
                "server_total_ms": 10,
                "query_execution_ms": 5
            }
        }"#;
        let resp: QueryResponse = serde_json::from_str(json).unwrap();
        let perf = resp.performance.unwrap();
        assert_eq!(perf.cache_hit_ratio, Some(0.95));
        assert_eq!(perf.cache_temperature, Some("hot".to_string()));
    }

    #[test]
    fn test_delete_all_response() {
        let json = r#"{"status": "ok"}"#;
        let resp: DeleteAllResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "ok");
    }

    #[test]
    fn test_namespace_metadata() {
        let json = r#"{
            "approx_count": 1000,
            "dimensions": 384,
            "created_at": "2024-01-15T12:00:00Z"
        }"#;
        let resp: NamespaceMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(resp.approx_count, Some(1000));
        assert_eq!(resp.dimensions, Some(384));
    }

    #[test]
    fn test_namespaces_response() {
        let json = r#"{
            "namespaces": [
                {"id": "ns1"},
                {"id": "ns2"}
            ],
            "next_cursor": "abc123"
        }"#;
        let resp: NamespacesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.namespaces.len(), 2);
        assert_eq!(resp.namespaces[0].id, "ns1");
        assert_eq!(resp.next_cursor, Some("abc123".to_string()));
    }

    #[test]
    fn test_multi_query_response() {
        let json = r#"{
            "results": [
                {"rows": [{"id": 1}]},
                {"rows": [{"id": 2}, {"id": 3}]}
            ]
        }"#;
        let resp: MultiQueryResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.results.len(), 2);
        assert_eq!(resp.results[0].rows.len(), 1);
        assert_eq!(resp.results[1].rows.len(), 2);
    }
}
