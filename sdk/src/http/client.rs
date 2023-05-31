use crate::client::Client;
use crate::error::Error;
use reqwest::Url;
use serde::Serialize;

#[derive(Debug)]
pub struct HttpClient {
    pub api_url: Url,
    client: reqwest::Client,
}

impl Client for HttpClient {}

unsafe impl Send for HttpClient {}
unsafe impl Sync for HttpClient {}

impl HttpClient {
    pub fn create(api_url: &str) -> Result<Self, Error> {
        let api_url = Url::parse(api_url);
        if api_url.is_err() {
            return Err(Error::CannotParseUrl);
        }
        let api_url = api_url.unwrap();
        let client = reqwest::Client::builder().build()?;
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
