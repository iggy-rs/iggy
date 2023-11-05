use crate::client::Client;
use crate::error::Error;
use crate::http::config::HttpClientConfig;
use crate::models::identity_info::IdentityInfo;
use async_trait::async_trait;
use reqwest::{Response, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    access_token: RwLock<String>,
    refresh_token: RwLock<String>,
}

#[async_trait]
impl Client for HttpClient {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn disconnect(&self) -> Result<(), Error> {
        Ok(())
    }
}

unsafe impl Send for HttpClient {}
unsafe impl Sync for HttpClient {}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient::create(Arc::new(HttpClientConfig::default())).unwrap()
    }
}

impl HttpClient {
    /// Create a new HTTP client for interacting with the Iggy API using the provided API URL.
    pub fn new(api_url: &str) -> Result<Self, Error> {
        Self::create(Arc::new(HttpClientConfig {
            api_url: api_url.to_string(),
            ..Default::default()
        }))
    }

    /// Create a new HTTP client for interacting with the Iggy API using the provided configuration.
    pub fn create(config: Arc<HttpClientConfig>) -> Result<Self, Error> {
        let api_url = Url::parse(&config.api_url);
        if api_url.is_err() {
            return Err(Error::CannotParseUrl);
        }
        let api_url = api_url.unwrap();
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(config.retries);
        let client = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        Ok(Self {
            api_url,
            client,
            access_token: RwLock::new("".to_string()),
            refresh_token: RwLock::new("".to_string()),
        })
    }

    /// Invoke HTTP GET request to the Iggy API.
    pub async fn get(&self, path: &str) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self.client.get(url).bearer_auth(token).send().await?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP GET request to the Iggy API with query parameters.
    pub async fn get_with_query<T: Serialize + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .query(query)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP POST request to the Iggy API.
    pub async fn post<T: Serialize + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .post(url)
            .bearer_auth(token)
            .json(payload)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP PUT request to the Iggy API.
    pub async fn put<T: Serialize + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .put(url)
            .bearer_auth(token)
            .json(payload)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP DELETE request to the Iggy API.
    pub async fn delete(&self, path: &str) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self.client.delete(url).bearer_auth(token).send().await?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP DELETE request to the Iggy API with query parameters.
    pub async fn delete_with_query<T: Serialize + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .delete(url)
            .bearer_auth(token)
            .query(query)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    /// Get full URL for the provided path.
    pub fn get_url(&self, path: &str) -> Result<Url, Error> {
        self.api_url.join(path).map_err(|_| Error::CannotParseUrl)
    }

    /// Returns true if the client is authenticated.
    pub async fn is_authenticated(&self) -> bool {
        let token = self.access_token.read().await;
        !token.is_empty()
    }

    /// Set the refresh token.
    pub async fn set_refresh_token(&self, token: Option<String>) {
        let mut current_token = self.refresh_token.write().await;
        if let Some(token) = token {
            *current_token = token;
        } else {
            *current_token = "".to_string();
        }
    }

    /// Set the access token.
    pub async fn set_access_token(&self, token: Option<String>) {
        let mut current_token = self.access_token.write().await;
        if let Some(token) = token {
            *current_token = token;
        } else {
            *current_token = "".to_string();
        }
    }

    /// Set the access token and refresh token from the provided identity.
    pub async fn set_tokens_from_identity(&self, identity: &IdentityInfo) -> Result<(), Error> {
        if identity.token.is_none() {
            return Err(Error::JwtMissing);
        }

        let token = identity.token.as_ref().unwrap();
        if token.access_token.is_empty() {
            return Err(Error::JwtMissing);
        }

        self.set_access_token(Some(token.access_token.clone()))
            .await;
        self.set_refresh_token(Some(token.refresh_token.clone()))
            .await;
        Ok(())
    }

    /// Refresh the access token using the provided refresh token.
    pub async fn refresh_access_token_using_current_refresh_token(&self) -> Result<(), Error> {
        let refresh_token = self.refresh_token.read().await;
        self.refresh_access_token(&refresh_token).await
    }

    async fn handle_response(response: Response) -> Result<Response, Error> {
        match response.status().is_success() {
            true => Ok(response),
            false => Err(Error::HttpResponseError(
                response.status().as_u16(),
                response.text().await.unwrap_or("error".to_string()),
            )),
        }
    }

    async fn fail_if_not_authenticated(&self, path: &str) -> Result<(), Error> {
        if UNAUTHORIZED_PATHS.contains(&path) {
            return Ok(());
        }
        if !self.is_authenticated().await {
            return Err(Error::Unauthenticated);
        }
        Ok(())
    }
}
