use crate::{Error, Namespace, NamespacesResponse, Result};

const DEFAULT_BASE_URL: &str = "https://api.turbopuffer.com";

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct NamespacesParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
}

pub struct Client {
    pub(crate) api_key: String,
    pub(crate) base_url: String,
    pub(crate) http: reqwest::Client,
}

impl Client {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_region(api_key: impl Into<String>, region: &str) -> Self {
        let base_url = format!("https://{}.turbopuffer.com", region);
        Self {
            api_key: api_key.into(),
            base_url,
            http: reqwest::Client::new(),
        }
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
            http: reqwest::Client::new(),
        }
    }

    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("TURBOPUFFER_API_KEY")
            .map_err(|_| Error::Api {
                status: 0,
                message: "TURBOPUFFER_API_KEY not set".to_string(),
            })?;

        let base_url = std::env::var("TURBOPUFFER_REGION")
            .map(|r| format!("https://{}.turbopuffer.com", r))
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());

        Ok(Self {
            api_key,
            base_url,
            http: reqwest::Client::new(),
        })
    }

    pub fn namespace(&self, name: impl Into<String>) -> Namespace<'_> {
        Namespace::new(self, name.into())
    }

    pub async fn namespaces(&self, params: NamespacesParams) -> Result<NamespacesResponse> {
        let mut query_parts = Vec::new();
        if let Some(ref prefix) = params.prefix {
            query_parts.push(format!("prefix={}", prefix));
        }
        if let Some(ref cursor) = params.cursor {
            query_parts.push(format!("cursor={}", cursor));
        }
        if let Some(page_size) = params.page_size {
            query_parts.push(format!("page_size={}", page_size));
        }

        let path = if query_parts.is_empty() {
            "/v1/namespaces".to_string()
        } else {
            format!("/v1/namespaces?{}", query_parts.join("&"))
        };

        self.request_no_body(reqwest::Method::GET, &path).await
    }

    pub(crate) async fn request<T, R>(&self, method: reqwest::Method, path: &str, body: Option<&T>) -> Result<R>
    where
        T: serde::Serialize + ?Sized,
        R: serde::de::DeserializeOwned,
    {
        let url = format!("{}{}", self.base_url, path);

        let mut req = self.http
            .request(method, &url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json");

        if let Some(body) = body {
            req = req.json(body);
        }

        let resp = req.send().await?;
        let status = resp.status();

        if !status.is_success() {
            let message = resp.text().await.unwrap_or_default();
            return Err(Error::Api {
                status: status.as_u16(),
                message,
            });
        }

        let result = resp.json().await?;
        Ok(result)
    }

    pub(crate) async fn request_no_body<R>(&self, method: reqwest::Method, path: &str) -> Result<R>
    where
        R: serde::de::DeserializeOwned,
    {
        self.request::<(), R>(method, path, None).await
    }
}
