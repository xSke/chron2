use std::time::Duration;

use async_trait::async_trait;
use chron_db::{models::EntityKind, NewObject};
use serde::Deserialize;
use uuid::Uuid;

use super::{IntervalWorker, WorkerContext};

pub struct PollPosts;

#[async_trait]
impl IntervalWorker for PollPosts {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(30))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
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
            }

            if page_idx >= page.total_pages - 1 {
                break;
            }
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
}
