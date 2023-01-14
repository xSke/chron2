use std::time::Duration;

use async_trait::async_trait;
use chron_db::models::EntityKind;
use futures::{stream, StreamExt};
use tokio::time::interval;

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

        stream::iter(find_urls(&elections.parse()?))
            .for_each(|url| {
                let ctx = &ctx;
                async move {
                    if let Err(e) = fetch_and_save_asset(&ctx, url.as_str()).await {
                        dbg!(e);
                    }
                }
            })
            .await;
        Ok(())
    }
}
