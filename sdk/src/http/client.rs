use crate::client::Client;
use crate::error::Error;
use crate::http::config::HttpClientConfig;
use async_trait::async_trait;
use reqwest::Url;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::Serialize;

#[derive(Debug)]
pub struct HttpClient {
    pub api_url: Url,
    client: ClientWithMiddleware,
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

impl HttpClient {
    pub fn new(api_url: &str) -> Result<Self, Error> {
        Self::create(HttpClientConfig {
            api_url: api_url.to_string(),
            ..Default::default()
        })
    }

    pub fn create(config: HttpClientConfig) -> Result<Self, Error> {
        let api_url = Url::parse(&config.api_url);
        if api_url.is_err() {
            return Err(Error::CannotParseUrl);
        }
        let api_url = api_url.unwrap();
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(config.retries);
        let client = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        Ok(Self { api_url, client })
    }

    pub async fn get(&self, path: &str) -> Result<reqwest::Response, Error> {
        let url = self.get_url(path)?;
        let response = self.client.get(url).send().await?;
        Ok(response)
    }

    pub async fn get_with_query<T: Serialize + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<reqwest::Response, Error> {
        let url = self.get_url(path)?;
        let response = self.client.get(url).query(query).send().await?;
        Ok(response)
    }

    pub async fn post<T: Serialize + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<reqwest::Response, Error> {
        let url = self.get_url(path)?;
        let response = self.client.post(url).json(payload).send().await?;
        Ok(response)
    }

    pub async fn put<T: Serialize + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<reqwest::Response, Error> {
        let url = self.get_url(path)?;
        let response = self.client.put(url).json(payload).send().await?;
        Ok(response)
    }

    pub async fn delete(&self, path: &str) -> Result<reqwest::Response, Error> {
        let url = self.get_url(path)?;
        let response = self.client.delete(url).send().await?;
        Ok(response)
    }

    pub fn get_url(&self, path: &str) -> Result<Url, Error> {
        self.api_url.join(path).map_err(|_| Error::CannotParseUrl)
    }
}
