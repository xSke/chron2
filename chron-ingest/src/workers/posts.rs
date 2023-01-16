use std::{collections::HashSet, time::Duration};

use async_trait::async_trait;
use chron_db::{models::EntityKind, NewObject};
use futures::TryFutureExt;
use serde::Deserialize;
use tracing::error;
use uuid::Uuid;

use crate::asset::fetch_and_save_asset;

use super::{IntervalWorker, WorkerContext};

pub struct PollPosts;

#[async_trait]
impl IntervalWorker for PollPosts {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(30))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let mut icons = HashSet::new();
        for page_idx in 0.. {
            let resp = ctx
                .client
                .fetch(&format!(
                    "https://api2.blaseball.com/feed?page={}",
                    page_idx
                ))
                .await?;
            let page = resp.parse::<PostPage>()?;
            for post_value in page.posts {
                let post = serde_json::from_value::<Post>(post_value.clone())?;

                ctx.db
                    .save(&NewObject {
                        kind: EntityKind::Post,
                        entity_id: post.id,
                        request_time: resp.request_time(),
                        timestamp: resp.timestamp(),
                        data: post_value,
                    })
                    .await?;

                if let Some(icon) = post.user.profile_pin_url {
                    icons.insert(icon);
                }
            }

            if page_idx >= page.total_pages - 1 {
                break;
            }
        }

        for url in icons {
            fetch_and_save_asset(ctx, &url)
                .unwrap_or_else(|e| {
                    error!("{}", e);
                })
                .await;
        }

        Ok(())
    }
}

#[derive(Deserialize)]
struct PostPage {
    #[serde(rename = "totalPages")]
    total_pages: i32,
    posts: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct Post {
    id: Uuid,
    user: PostUser,
}

#[derive(Deserialize)]
struct PostUser {
    #[serde(rename = "profilePinUrl")]
    profile_pin_url: Option<String>,
}
