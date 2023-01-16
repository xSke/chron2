use std::{hash::Hasher, str::FromStr};

use base64::Engine;
use chron_base::uuid_hash;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use siphasher::sip128::{Hasher128, SipHasher};
use uuid::Uuid;

use crate::workers::WorkerContext;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Asset {
    pub id: Uuid,
    pub url: String,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub hash: Uuid,
    pub data: String, // base64 string. todo: remove this, move to different table
}

impl Asset {
    pub fn new(
        url: Url,
        last_modified: Option<String>,
        content_type: Option<String>,
        data: &[u8],
    ) -> Asset {
        let mut hasher = SipHasher::new();
        hasher.write(url.to_string().as_bytes());

        let url_hash = Uuid::from_u128(hasher.finish128().as_u128());

        let engine = base64::engine::general_purpose::STANDARD;
        let encoded_data = engine.encode(data);

        let data_hash = uuid_hash(data);

        Asset {
            id: url_hash,
            last_modified,
            content_type,
            url: url.to_string(),
            hash: data_hash,
            data: encoded_data,
        }
    }
}

pub fn find_urls(value: &serde_json::Value) -> Vec<Url> {
    fn find_urls_inner(value: &serde_json::Value, urls: &mut Vec<Url>) {
        match value {
            serde_json::Value::Array(arr) => {
                for val in arr {
                    find_urls_inner(val, urls);
                }
            }
            serde_json::Value::Object(obj) => {
                for val in obj.values() {
                    find_urls_inner(val, urls);
                }
            }
            serde_json::Value::String(str) => {
                if let Ok(url) = Url::from_str(str) {
                    if url.has_host() {
                        urls.push(url);
                    }
                }
            }
            _ => {}
        }
    }

    let mut urls = Vec::new();
    find_urls_inner(value, &mut urls);
    urls
}

pub async fn fetch_and_save_asset(ctx: &WorkerContext, url: &str) -> anyhow::Result<()> {
    let asset = ctx.client.fetch(url).await?;
    ctx.db.save(&asset.to_asset_object()?).await?;
    Ok(())
}
