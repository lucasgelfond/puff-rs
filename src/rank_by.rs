use serde::ser::{SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bm25Params {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_as_prefix: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RankBy {
    // Vector search: ["attr", "ANN", [vector]]
    Vector { attr: String, query: Vec<f32> },
    // Exact kNN: ["attr", "kNN", [vector]]
    VectorKnn { attr: String, query: Vec<f32> },
    // BM25 text search: ["attr", "BM25", "query"]
    Bm25 { attr: String, query: String, params: Option<Bm25Params> },
    // Attribute ordering: ["attr", "asc"|"desc"]
    Attribute { attr: String, order: Order },
    // Combinators
    Sum(Vec<RankBy>),
    Max(Vec<RankBy>),
    Product { weight: f64, subquery: Box<RankBy> },
}

impl RankBy {
    pub fn vector(attr: impl Into<String>, query: Vec<f32>) -> Self {
        RankBy::Vector { attr: attr.into(), query }
    }

    pub fn vector_knn(attr: impl Into<String>, query: Vec<f32>) -> Self {
        RankBy::VectorKnn { attr: attr.into(), query }
    }

    pub fn bm25(attr: impl Into<String>, query: impl Into<String>) -> Self {
        RankBy::Bm25 { attr: attr.into(), query: query.into(), params: None }
    }

    pub fn bm25_with_params(attr: impl Into<String>, query: impl Into<String>, params: Bm25Params) -> Self {
        RankBy::Bm25 { attr: attr.into(), query: query.into(), params: Some(params) }
    }

    pub fn attribute(attr: impl Into<String>, order: Order) -> Self {
        RankBy::Attribute { attr: attr.into(), order }
    }

    pub fn asc(attr: impl Into<String>) -> Self {
        RankBy::Attribute { attr: attr.into(), order: Order::Asc }
    }

    pub fn desc(attr: impl Into<String>) -> Self {
        RankBy::Attribute { attr: attr.into(), order: Order::Desc }
    }

    pub fn sum(subqueries: Vec<RankBy>) -> Self {
        RankBy::Sum(subqueries)
    }

    pub fn max(subqueries: Vec<RankBy>) -> Self {
        RankBy::Max(subqueries)
    }

    pub fn product(weight: f64, subquery: RankBy) -> Self {
        RankBy::Product { weight, subquery: Box::new(subquery) }
    }
}

impl Serialize for RankBy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            RankBy::Vector { attr, query } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("ANN")?;
                seq.serialize_element(query)?;
                seq.end()
            }
            RankBy::VectorKnn { attr, query } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("kNN")?;
                seq.serialize_element(query)?;
                seq.end()
            }
            RankBy::Bm25 { attr, query, params } => {
                if let Some(p) = params {
                    let mut seq = serializer.serialize_seq(Some(4))?;
                    seq.serialize_element(attr)?;
                    seq.serialize_element("BM25")?;
                    seq.serialize_element(query)?;
                    seq.serialize_element(p)?;
                    seq.end()
                } else {
                    let mut seq = serializer.serialize_seq(Some(3))?;
                    seq.serialize_element(attr)?;
                    seq.serialize_element("BM25")?;
                    seq.serialize_element(query)?;
                    seq.end()
                }
            }
            RankBy::Attribute { attr, order } => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(attr)?;
                seq.serialize_element(order)?;
                seq.end()
            }
            RankBy::Sum(subqueries) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("Sum")?;
                seq.serialize_element(subqueries)?;
                seq.end()
            }
            RankBy::Max(subqueries) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("Max")?;
                seq.serialize_element(subqueries)?;
                seq.end()
            }
            RankBy::Product { weight, subquery } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element("Product")?;
                seq.serialize_element(weight)?;
                seq.serialize_element(subquery)?;
                seq.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_serialization() {
        let r = RankBy::vector("vector", vec![0.1, 0.2, 0.3]);
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["vector","ANN",[0.1,0.2,0.3]]"#);
    }

    #[test]
    fn test_vector_knn_serialization() {
        let r = RankBy::vector_knn("embedding", vec![1.0, 2.0, 3.0]);
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["embedding","kNN",[1.0,2.0,3.0]]"#);
    }

    #[test]
    fn test_bm25_serialization() {
        let r = RankBy::bm25("content", "quick fox");
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["content","BM25","quick fox"]"#);
    }

    #[test]
    fn test_bm25_with_params_serialization() {
        let r = RankBy::bm25_with_params("content", "quick", Bm25Params {
            last_as_prefix: Some(true),
        });
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["content","BM25","quick",{"last_as_prefix":true}]"#);
    }

    #[test]
    fn test_attribute_asc_serialization() {
        let r = RankBy::asc("timestamp");
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["timestamp","asc"]"#);
    }

    #[test]
    fn test_attribute_desc_serialization() {
        let r = RankBy::desc("timestamp");
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["timestamp","desc"]"#);
    }

    #[test]
    fn test_attribute_with_order_serialization() {
        let r = RankBy::attribute("score", Order::Desc);
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["score","desc"]"#);
    }

    #[test]
    fn test_sum_serialization() {
        let r = RankBy::sum(vec![
            RankBy::bm25("title", "fox"),
            RankBy::bm25("content", "fox"),
        ]);
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["Sum",[["title","BM25","fox"],["content","BM25","fox"]]]"#);
    }

    #[test]
    fn test_max_serialization() {
        let r = RankBy::max(vec![
            RankBy::bm25("title", "query"),
            RankBy::vector("vec", vec![0.1, 0.2]),
        ]);
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["Max",[["title","BM25","query"],["vec","ANN",[0.1,0.2]]]]"#);
    }

    #[test]
    fn test_product_serialization() {
        let r = RankBy::product(2.0, RankBy::bm25("title", "fox"));
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["Product",2.0,["title","BM25","fox"]]"#);
    }

    #[test]
    fn test_nested_combinators() {
        let r = RankBy::sum(vec![
            RankBy::product(2.0, RankBy::bm25("title", "query")),
            RankBy::product(1.0, RankBy::bm25("content", "query")),
        ]);
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(
            json,
            r#"["Sum",[["Product",2.0,["title","BM25","query"]],["Product",1.0,["content","BM25","query"]]]]"#
        );
    }

    #[test]
    fn test_empty_vector() {
        let r = RankBy::vector("vec", vec![]);
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"["vec","ANN",[]]"#);
    }
}
