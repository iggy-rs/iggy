use crate::client::Client;
use crate::client_v2::ClientV2;
use crate::error::IggyError;
use crate::http::config::HttpClientConfig;
use crate::http::HttpTransport;
use crate::locking::{IggySharedMut, IggySharedMutFn};
use crate::models::identity_info::IdentityInfo;
use async_trait::async_trait;
use reqwest::{Response, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::Serialize;
use std::ops::Deref;
use std::sync::Arc;

const UNAUTHORIZED_PATHS: &[&str] = &[
    "/",
    "/metrics",
    "/ping",
    "/users/login",
    "/users/refresh-token",
    "/personal-access-tokens/login",
];

/// HTTP client for interacting with the Iggy API.
/// It requires a valid API URL.
#[derive(Debug)]
pub struct HttpClient {
    /// The URL of the Iggy API.
    pub api_url: Url,
    client: ClientWithMiddleware,
    access_token: IggySharedMut<String>,
    refresh_token: IggySharedMut<String>,
}

#[async_trait]
impl Client for HttpClient {
    async fn connect(&self) -> Result<(), IggyError> {
        HttpClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        HttpClient::disconnect(self).await
    }
}

#[async_trait]
impl ClientV2 for HttpClient {
    async fn connect(&self) -> Result<(), IggyError> {
        HttpClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        HttpClient::disconnect(self).await
    }
}

unsafe impl Send for HttpClient {}
unsafe impl Sync for HttpClient {}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient::create(Arc::new(HttpClientConfig::default())).unwrap()
    }
}

#[async_trait]
impl HttpTransport for HttpClient {
    /// Get full URL for the provided path.
    fn get_url(&self, path: &str) -> Result<Url, IggyError> {
        self.api_url
            .join(path)
            .map_err(|_| IggyError::CannotParseUrl)
    }

    /// Invoke HTTP GET request to the Iggy API.
    async fn get(&self, path: &str) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .get(url)
            .bearer_auth(token.deref())
            .send()
            .await?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP GET request to the Iggy API with query parameters.
    async fn get_with_query<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .get(url)
            .bearer_auth(token.deref())
            .query(query)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP POST request to the Iggy API.
    async fn post<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .post(url)
            .bearer_auth(token.deref())
            .json(payload)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP PUT request to the Iggy API.
    async fn put<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .put(url)
            .bearer_auth(token.deref())
            .json(payload)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP DELETE request to the Iggy API.
    async fn delete(&self, path: &str) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .delete(url)
            .bearer_auth(token.deref())
            .send()
            .await?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP DELETE request to the Iggy API with query parameters.
    async fn delete_with_query<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .delete(url)
            .bearer_auth(token.deref())
            .query(query)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    /// Returns true if the client is authenticated.
    async fn is_authenticated(&self) -> bool {
        let token = self.access_token.read().await;
        !token.is_empty()
    }

    /// Refresh the access token using the provided refresh token.
    async fn refresh_access_token(&self, refresh_token: &str) -> Result<(), IggyError> {
        if refresh_token.is_empty() {
            return Err(IggyError::RefreshTokenMissing);
        }

        let command = RefreshToken {
            refresh_token: refresh_token.to_string(),
        };
        let response = self.post("/users/refresh-token", &command).await?;
        let identity_info: IdentityInfo = response.json().await?;
        if identity_info.tokens.is_none() {
            return Err(IggyError::JwtMissing);
        }

        self.set_tokens_from_identity(&identity_info).await?;
        Ok(())
    }

    /// Set the refresh token.
    async fn set_refresh_token(&self, token: Option<String>) {
        let mut current_token = self.refresh_token.write().await;
        if let Some(token) = token {
            *current_token = token;
        } else {
            *current_token = "".to_string();
        }
    }

    /// Set the access token.
    async fn set_access_token(&self, token: Option<String>) {
        let mut current_token = self.access_token.write().await;
        if let Some(token) = token {
            *current_token = token;
        } else {
            *current_token = "".to_string();
        }
    }

    /// Set the access token and refresh token from the provided identity.
    async fn set_tokens_from_identity(&self, identity: &IdentityInfo) -> Result<(), IggyError> {
        if identity.tokens.is_none() {
            return Err(IggyError::JwtMissing);
        }

        let tokens = identity.tokens.as_ref().unwrap();
        if tokens.access_token.token.is_empty() {
            return Err(IggyError::JwtMissing);
        }

        self.set_access_token(Some(tokens.access_token.token.clone()))
            .await;
        self.set_refresh_token(Some(tokens.refresh_token.token.clone()))
            .await;
        Ok(())
    }

    /// Refresh the access token using the provided refresh token.
    async fn refresh_access_token_using_current_refresh_token(&self) -> Result<(), IggyError> {
        let refresh_token = self.refresh_token.read().await;
        self.refresh_access_token(&refresh_token).await
    }
}

impl HttpClient {
    /// Create a new HTTP client for interacting with the Iggy API using the provided API URL.
    pub fn new(api_url: &str) -> Result<Self, IggyError> {
        Self::create(Arc::new(HttpClientConfig {
            api_url: api_url.to_string(),
            ..Default::default()
        }))
    }

    /// Create a new HTTP client for interacting with the Iggy API using the provided configuration.
    pub fn create(config: Arc<HttpClientConfig>) -> Result<Self, IggyError> {
        let api_url = Url::parse(&config.api_url);
        if api_url.is_err() {
            return Err(IggyError::CannotParseUrl);
        }
        let api_url = api_url.unwrap();
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(config.retries);
        let client = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        Ok(Self {
            api_url,
            client,
            access_token: IggySharedMut::new("".to_string()),
            refresh_token: IggySharedMut::new("".to_string()),
        })
    }

    async fn handle_response(response: Response) -> Result<Response, IggyError> {
        match response.status().is_success() {
            true => Ok(response),
            false => Err(IggyError::HttpResponseError(
                response.status().as_u16(),
                response.text().await.unwrap_or("error".to_string()),
            )),
        }
    }

    async fn fail_if_not_authenticated(&self, path: &str) -> Result<(), IggyError> {
        if UNAUTHORIZED_PATHS.contains(&path) {
            return Ok(());
        }
        if !self.is_authenticated().await {
            return Err(IggyError::Unauthenticated);
        }
        Ok(())
    }

    async fn connect(&self) -> Result<(), IggyError> {
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct RefreshToken {
    refresh_token: String,
}
