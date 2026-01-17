use serde::ser::{SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ContainsAllTokensParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_as_prefix: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    // Comparison ops: ["attr", "Op", value]
    Eq { attr: String, value: serde_json::Value },
    NotEq { attr: String, value: serde_json::Value },
    Lt { attr: String, value: serde_json::Value },
    Lte { attr: String, value: serde_json::Value },
    Gt { attr: String, value: serde_json::Value },
    Gte { attr: String, value: serde_json::Value },

    // Array element comparison
    AnyLt { attr: String, value: serde_json::Value },
    AnyLte { attr: String, value: serde_json::Value },
    AnyGt { attr: String, value: serde_json::Value },
    AnyGte { attr: String, value: serde_json::Value },

    // Set ops: ["attr", "Op", [values]]
    In { attr: String, values: Vec<serde_json::Value> },
    NotIn { attr: String, values: Vec<serde_json::Value> },
    Contains { attr: String, value: serde_json::Value },
    NotContains { attr: String, value: serde_json::Value },
    ContainsAny { attr: String, values: Vec<serde_json::Value> },
    NotContainsAny { attr: String, values: Vec<serde_json::Value> },

    // String ops
    Glob { attr: String, pattern: String },
    NotGlob { attr: String, pattern: String },
    IGlob { attr: String, pattern: String },
    NotIGlob { attr: String, pattern: String },
    Regex { attr: String, pattern: String },

    // FTS ops
    ContainsAllTokens { attr: String, value: String, params: Option<ContainsAllTokensParams> },
    ContainsTokenSequence { attr: String, value: String },

    // Logical ops
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

impl Filter {
    pub fn eq(attr: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        Filter::Eq { attr: attr.into(), value: value.into() }
    }

    pub fn not_eq(attr: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        Filter::NotEq { attr: attr.into(), value: value.into() }
    }

    pub fn lt(attr: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        Filter::Lt { attr: attr.into(), value: value.into() }
    }

    pub fn lte(attr: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        Filter::Lte { attr: attr.into(), value: value.into() }
    }

    pub fn gt(attr: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        Filter::Gt { attr: attr.into(), value: value.into() }
    }

    pub fn gte(attr: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        Filter::Gte { attr: attr.into(), value: value.into() }
    }

    pub fn r#in(attr: impl Into<String>, values: Vec<serde_json::Value>) -> Self {
        Filter::In { attr: attr.into(), values }
    }

    pub fn not_in(attr: impl Into<String>, values: Vec<serde_json::Value>) -> Self {
        Filter::NotIn { attr: attr.into(), values }
    }

    pub fn contains(attr: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        Filter::Contains { attr: attr.into(), value: value.into() }
    }

    pub fn contains_any(attr: impl Into<String>, values: Vec<serde_json::Value>) -> Self {
        Filter::ContainsAny { attr: attr.into(), values }
    }

    pub fn glob(attr: impl Into<String>, pattern: impl Into<String>) -> Self {
        Filter::Glob { attr: attr.into(), pattern: pattern.into() }
    }

    pub fn iglob(attr: impl Into<String>, pattern: impl Into<String>) -> Self {
        Filter::IGlob { attr: attr.into(), pattern: pattern.into() }
    }

    pub fn regex(attr: impl Into<String>, pattern: impl Into<String>) -> Self {
        Filter::Regex { attr: attr.into(), pattern: pattern.into() }
    }

    pub fn and(filters: Vec<Filter>) -> Self {
        Filter::And(filters)
    }

    pub fn or(filters: Vec<Filter>) -> Self {
        Filter::Or(filters)
    }

    pub fn not(filter: Filter) -> Self {
        Filter::Not(Box::new(filter))
    }
}

impl Serialize for Filter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            // Binary ops: ["attr", "Op", value]
            Filter::Eq { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("Eq")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::NotEq { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("NotEq")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::Lt { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("Lt")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::Lte { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("Lte")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::Gt { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("Gt")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::Gte { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("Gte")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::AnyLt { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("AnyLt")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::AnyLte { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("AnyLte")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::AnyGt { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("AnyGt")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::AnyGte { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("AnyGte")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::In { attr, values } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("In")?;
                seq.serialize_element(values)?;
                seq.end()
            }
            Filter::NotIn { attr, values } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("NotIn")?;
                seq.serialize_element(values)?;
                seq.end()
            }
            Filter::Contains { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("Contains")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::NotContains { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("NotContains")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            Filter::ContainsAny { attr, values } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("ContainsAny")?;
                seq.serialize_element(values)?;
                seq.end()
            }
            Filter::NotContainsAny { attr, values } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("NotContainsAny")?;
                seq.serialize_element(values)?;
                seq.end()
            }
            Filter::Glob { attr, pattern } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("Glob")?;
                seq.serialize_element(pattern)?;
                seq.end()
            }
            Filter::NotGlob { attr, pattern } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("NotGlob")?;
                seq.serialize_element(pattern)?;
                seq.end()
            }
            Filter::IGlob { attr, pattern } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("IGlob")?;
                seq.serialize_element(pattern)?;
                seq.end()
            }
            Filter::NotIGlob { attr, pattern } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("NotIGlob")?;
                seq.serialize_element(pattern)?;
                seq.end()
            }
            Filter::Regex { attr, pattern } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("Regex")?;
                seq.serialize_element(pattern)?;
                seq.end()
            }
            Filter::ContainsAllTokens { attr, value, params } => {
                if let Some(p) = params {
                    let mut seq = serializer.serialize_seq(Some(4))?;
                    seq.serialize_element(attr)?;
                    seq.serialize_element("ContainsAllTokens")?;
                    seq.serialize_element(value)?;
                    seq.serialize_element(p)?;
                    seq.end()
                } else {
                    let mut seq = serializer.serialize_seq(Some(3))?;
                    seq.serialize_element(attr)?;
                    seq.serialize_element("ContainsAllTokens")?;
                    seq.serialize_element(value)?;
                    seq.end()
                }
            }
            Filter::ContainsTokenSequence { attr, value } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(attr)?;
                seq.serialize_element("ContainsTokenSequence")?;
                seq.serialize_element(value)?;
                seq.end()
            }
            // Logical ops
            Filter::And(filters) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("And")?;
                seq.serialize_element(filters)?;
                seq.end()
            }
            Filter::Or(filters) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("Or")?;
                seq.serialize_element(filters)?;
                seq.end()
            }
            Filter::Not(filter) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("Not")?;
                seq.serialize_element(filter)?;
                seq.end()
            }
        }
    }
}

impl Serialize for ContainsAllTokensParams {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        if let Some(v) = self.last_as_prefix {
            map.serialize_entry("last_as_prefix", &v)?;
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq_serialization() {
        let f = Filter::eq("name", "foo");
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["name","Eq","foo"]"#);
    }

    #[test]
    fn test_not_eq_serialization() {
        let f = Filter::not_eq("status", "deleted");
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["status","NotEq","deleted"]"#);
    }

    #[test]
    fn test_comparison_ops() {
        assert_eq!(
            serde_json::to_string(&Filter::lt("age", 30)).unwrap(),
            r#"["age","Lt",30]"#
        );
        assert_eq!(
            serde_json::to_string(&Filter::lte("age", 30)).unwrap(),
            r#"["age","Lte",30]"#
        );
        assert_eq!(
            serde_json::to_string(&Filter::gt("score", 0.5)).unwrap(),
            r#"["score","Gt",0.5]"#
        );
        assert_eq!(
            serde_json::to_string(&Filter::gte("score", 0.5)).unwrap(),
            r#"["score","Gte",0.5]"#
        );
    }

    #[test]
    fn test_and_serialization() {
        let f = Filter::and(vec![
            Filter::eq("name", "foo"),
            Filter::gt("age", 18),
        ]);
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["And",[["name","Eq","foo"],["age","Gt",18]]]"#);
    }

    #[test]
    fn test_or_serialization() {
        let f = Filter::or(vec![
            Filter::eq("role", "admin"),
            Filter::eq("role", "mod"),
        ]);
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["Or",[["role","Eq","admin"],["role","Eq","mod"]]]"#);
    }

    #[test]
    fn test_not_serialization() {
        let f = Filter::not(Filter::eq("deleted", true));
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["Not",["deleted","Eq",true]]"#);
    }

    #[test]
    fn test_in_serialization() {
        let f = Filter::r#in("status", vec!["active".into(), "pending".into()]);
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["status","In",["active","pending"]]"#);
    }

    #[test]
    fn test_not_in_serialization() {
        let f = Filter::not_in("status", vec!["deleted".into()]);
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["status","NotIn",["deleted"]]"#);
    }

    #[test]
    fn test_contains_serialization() {
        let f = Filter::contains("tags", "rust");
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["tags","Contains","rust"]"#);
    }

    #[test]
    fn test_contains_any_serialization() {
        let f = Filter::contains_any("tags", vec!["rust".into(), "go".into()]);
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["tags","ContainsAny",["rust","go"]]"#);
    }

    #[test]
    fn test_glob_serialization() {
        let f = Filter::glob("name", "a*");
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["name","Glob","a*"]"#);
    }

    #[test]
    fn test_iglob_serialization() {
        let f = Filter::iglob("name", "A*");
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["name","IGlob","A*"]"#);
    }

    #[test]
    fn test_regex_serialization() {
        let f = Filter::regex("email", r".*@.*\.com");
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["email","Regex",".*@.*\\.com"]"#);
    }

    #[test]
    fn test_nested_logical_ops() {
        let f = Filter::and(vec![
            Filter::or(vec![
                Filter::eq("a", 1),
                Filter::eq("b", 2),
            ]),
            Filter::not(Filter::eq("c", 3)),
        ]);
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(
            json,
            r#"["And",[["Or",[["a","Eq",1],["b","Eq",2]]],["Not",["c","Eq",3]]]]"#
        );
    }

    #[test]
    fn test_numeric_values() {
        let f = Filter::eq("count", 42);
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["count","Eq",42]"#);

        let f = Filter::eq("price", 19.99);
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["price","Eq",19.99]"#);
    }

    #[test]
    fn test_null_value() {
        let f = Filter::Eq { attr: "field".into(), value: serde_json::Value::Null };
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, r#"["field","Eq",null]"#);
    }
}
