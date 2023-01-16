use async_trait::async_trait;
use chron_db::models::EntityKind;
use std::time::Duration;
use uuid::Uuid;

use crate::asset::{fetch_and_save_asset, find_urls};

use super::{IntervalWorker, WorkerContext};

pub struct PollAssets;

#[async_trait]
impl IntervalWorker for PollAssets {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        save_book(ctx).await?;

        // todo: avoid duplication for where else we're saving this
        let flagsmith = ctx
            .client
            .fetch(&format!("https://api2.blaseball.com/flagsmith"))
            .await?;
        for url in find_urls(&flagsmith.parse()?) {
            fetch_and_save_asset(ctx, url.as_str()).await?;
        }

        Ok(())
    }
}

async fn save_book(ctx: &WorkerContext) -> anyhow::Result<()> {
    let book = ctx
        .client
        .fetch(&format!(
            "https://blaseball-texts.s3.us-west-2.amazonaws.com/forbiddenbook.json"
        ))
        .await?;
    ctx.db
        .save(&book.to_chron(EntityKind::ForbiddenBook, Uuid::default())?)
        .await?;
    ctx.db.save(&book.to_asset_object()?).await?;
    Ok(())
}
