use std::sync::Arc;

use chron_db::{models::EntityKind, NewObject};
use dashmap::DashMap;
use reqwest::{
    header::{self, HeaderMap, HeaderValue},
    Client, ClientBuilder, StatusCode, Url,
};
use serde::de::DeserializeOwned;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
pub struct DataClient {
    client: Client,
    cached_responses: Arc<DashMap<String, ClientResponse>>,
}

#[derive(Debug, Clone)]
pub struct ClientResponse {
    pub url: Url,
    pub timestamp_before: OffsetDateTime,
    pub timestamp_after: OffsetDateTime,
    pub etag: Option<String>,
    pub data: Vec<u8>,
    pub status_code: StatusCode,
    pub was_cached: bool,
}

impl ClientResponse {
    pub fn parse<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        Ok(serde_json::from_slice(&self.data)?)
    }

    pub fn to_chron(&self, kind: EntityKind, entity_id: Uuid) -> anyhow::Result<NewObject> {
        let parsed = serde_json::from_slice(&self.data)?;

        Ok(NewObject {
            data: parsed,
            kind,
            entity_id,
            request_time: self.timestamp_after - self.timestamp_before,
            timestamp: self.timestamp_before,
        })
    }

    pub fn request_time(&self) -> time::Duration {
        self.timestamp_after - self.timestamp_before
    }

    pub fn timestamp(&self) -> OffsetDateTime {
        self.timestamp_before
    }
}

impl DataClient {
    pub fn new(cookie: &str) -> anyhow::Result<DataClient> {
        let mut headers = HeaderMap::new();

        // i'm too tired for this
        headers.insert("Cookie", HeaderValue::from_str(cookie.trim())?);

        let client = ClientBuilder::new()
            .user_agent("archiver/0.2 (hello umps this is sibr, please do not ban us)")
            .deflate(true)
            .brotli(true)
            .gzip(true)
            .default_headers(headers)
            .build()?;

        Ok(DataClient {
            client,
            cached_responses: Arc::new(DashMap::new()),
        })
    }

    pub async fn fetch(&self, orig_url: &str) -> anyhow::Result<ClientResponse> {
        let mut request = self.client.get(orig_url);
        if let Some(cached_etag) = self
            .cached_responses
            .get(orig_url)
            .and_then(|x| x.etag.clone())
        {
            request = request.header(header::IF_NONE_MATCH, cached_etag);
        }

        let timestamp_before = OffsetDateTime::now_utc();
        let response = request.send().await?;
        let timestamp_after = OffsetDateTime::now_utc();

        let url = response.url().clone();
        // let server_date = response
        //     .headers()
        //     .get(header::DATE)
        //     .and_then(|x| x.to_str().ok())
        //     .map(|x| x.to_owned());
        let etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|x| x.to_str().ok())
            .map(|x| x.to_owned());
        let status_code = response.status();

        println!(
            "{} {} ({}s)",
            response.status(),
            response.url().to_string(),
            (timestamp_after - timestamp_before).as_seconds_f64()
        );

        let response = response.error_for_status()?;

        if response.status() == StatusCode::NOT_MODIFIED {
            if let Some(resp) = self.cached_responses.get(orig_url) {
                if resp.etag == etag {
                    let mut cached_resp = resp.clone();
                    cached_resp.status_code = response.status();
                    cached_resp.was_cached = true;
                    return Ok(cached_resp);
                }
            }
        }

        let data = response.bytes().await?.to_vec();

        let sr = ClientResponse {
            url,
            timestamp_before,
            timestamp_after,
            etag,
            data,
            status_code,
            was_cached: false,
        };

        if sr.etag.is_some() {
            self.cached_responses
                .insert(orig_url.to_string(), sr.clone());
        }

        Ok(sr)
    }
}
