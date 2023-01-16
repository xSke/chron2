use std::time::Duration;

use async_trait::async_trait;
use chron_db::models::EntityKind;
use futures::TryFutureExt;
use tokio::time::interval;
use tracing::error;

use crate::asset::{fetch_and_save_asset, find_urls};

use super::{IntervalWorker, WorkerContext};

pub struct PollElections;

#[async_trait]
impl IntervalWorker for PollElections {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let (season, _) = ctx.season_day();
        let elections = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/elections",
                season
            ))
            .await?;
        ctx.db
            .save(&elections.to_chron(EntityKind::SeasonElections, season)?)
            .await?;

        for url in find_urls(&elections.parse()?) {
            fetch_and_save_asset(&ctx, url.as_str())
                .unwrap_or_else(|e| error!("{}", e))
                .await;
        }

        Ok(())
    }
}
