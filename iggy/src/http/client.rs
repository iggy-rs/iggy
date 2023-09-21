use crate::client::Client;
use crate::error::Error;
use crate::http::config::HttpClientConfig;
use async_trait::async_trait;
use reqwest::{Response, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct HttpClient {
    pub api_url: Url,
    client: ClientWithMiddleware,
    token: RwLock<String>,
}

#[async_trait]
impl Client for HttpClient {
    async fn connect(&mut self) -> Result<(), Error> {
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<(), Error> {
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
    pub fn new(api_url: &str) -> Result<Self, Error> {
        Self::create(Arc::new(HttpClientConfig {
            api_url: api_url.to_string(),
            ..Default::default()
        }))
    }

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
            token: RwLock::new("".to_string()),
        })
    }

    pub async fn get(&self, path: &str) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        let token = self.token.read().await;
        let response = self.client.get(url).bearer_auth(token).send().await?;
        Self::handle_response(response).await
    }

    pub async fn get_with_query<T: Serialize + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        let token = self.token.read().await;
        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .query(query)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    pub async fn post<T: Serialize + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        let token = self.token.read().await;
        let response = self
            .client
            .post(url)
            .bearer_auth(token)
            .json(payload)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    pub async fn put<T: Serialize + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        let token = self.token.read().await;
        let response = self
            .client
            .put(url)
            .bearer_auth(token)
            .json(payload)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    pub async fn delete(&self, path: &str) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        let token = self.token.read().await;
        let response = self.client.delete(url).bearer_auth(token).send().await?;
        Self::handle_response(response).await
    }

    pub async fn delete_with_query<T: Serialize + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, Error> {
        let url = self.get_url(path)?;
        let token = self.token.read().await;
        let response = self
            .client
            .delete(url)
            .bearer_auth(token)
            .query(query)
            .send()
            .await?;
        Self::handle_response(response).await
    }

    pub fn get_url(&self, path: &str) -> Result<Url, Error> {
        self.api_url.join(path).map_err(|_| Error::CannotParseUrl)
    }

    pub async fn set_token(&self, token: Option<String>) {
        let mut current_token = self.token.write().await;
        if let Some(token) = token {
            *current_token = token;
        } else {
            *current_token = "".to_string();
        }
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
}
