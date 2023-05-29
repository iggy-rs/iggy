use crate::error::Error;
use reqwest::Url;
use serde::Serialize;

pub struct Client {
    pub base_url: Url,
    client: reqwest::Client,
}

impl Client {
    pub fn create(base_url: &str) -> Result<Self, Error> {
        let base_url = Url::parse(base_url);
        if base_url.is_err() {
            return Err(Error::CannotParseUrl);
        }
        let base_url = base_url.unwrap();
        let client = reqwest::Client::builder().build()?;
        Ok(Self { base_url, client })
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

    pub async fn delete(&self, path: &str) -> Result<reqwest::Response, Error> {
        let url = self.get_url(path)?;
        let response = self.client.delete(url).send().await?;
        Ok(response)
    }

    pub fn get_url(&self, path: &str) -> Result<Url, Error> {
        self.base_url.join(path).map_err(|_| Error::CannotParseUrl)
    }
}
