use std::time::Duration;

use async_trait::async_trait;
use chron_db::{models::EntityKind, NewObject};
use futures::{stream, StreamExt, TryFutureExt};
use serde::Deserialize;
use tracing::error;
use uuid::Uuid;

use super::{IntervalWorker, WorkerContext};

pub struct PollSchedule;

#[async_trait]
impl IntervalWorker for PollSchedule {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(120))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let (season, _) = ctx.season_day();
        let resp = ctx
            .client
            .fetch(&format!("https://api2.blaseball.com/schedule/{}", season))
            .await?;

        let x = resp.parse::<Vec<ScheduleDay>>()?;
        stream::iter(x)
            .map(|d| {
                self.fetch_schedule_hourly(ctx.clone(), d.local_date)
                    .unwrap_or_else(|e| error!("{}", e))
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;

        Ok(())
    }
}

impl PollSchedule {
    async fn fetch_schedule_hourly(&self, ctx: WorkerContext, date: String) -> anyhow::Result<()> {
        let (season, _) = ctx.season_day();
        let resp = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/schedule/{}/{}/hourly",
                season, date
            ))
            .await?;

        for game_value in resp
            .parse::<Vec<ScheduleHour>>()?
            .into_iter()
            .flat_map(|x| x.bet_datas)
        {
            let game_id = serde_json::from_value::<BetData>(game_value.clone())?.game_id;

            ctx.db
                .save(&NewObject {
                    kind: EntityKind::GameBetData,
                    entity_id: game_id,
                    timestamp: resp.timestamp(),
                    request_time: resp.request_time(),
                    data: game_value,
                })
                .await?;
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct ScheduleDay {
    #[serde(rename = "localDate")]
    local_date: String,
}

#[derive(Debug, Deserialize)]
struct ScheduleHour {
    #[serde(rename = "betDatas")]
    bet_datas: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct BetData {
    #[serde(rename = "gameId")]
    game_id: Uuid,
}
