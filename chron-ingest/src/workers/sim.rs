use std::time::Duration;

use async_trait::async_trait;
use chron_db::models::EntityKind;
use serde::Deserialize;
use tokio::time::interval;
use uuid::Uuid;

use super::{IntervalWorker, WorkerContext};

pub struct PollSimData;

#[async_trait]
impl IntervalWorker for PollSimData {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(15))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        get_and_update_sim(ctx).await?;

        let flagsmith = ctx
            .client
            .fetch(&format!("https://api2.blaseball.com/flagsmith"))
            .await?;
        ctx.db
            .save(&flagsmith.to_chron(EntityKind::Flagsmith, Uuid::default())?)
            .await?;

        // so, as far as i can tell, the user id here isn't actually checked or ever validated
        // and everyone has the same ticker...?
        // but this is umpdog@sibr.dev
        let ticker = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/user-ticker/user/be2e2189-85e1-400b-ad24-e717fb6483a5"
            ))
            .await?;
        ctx.db
            .save(&ticker.to_chron(EntityKind::Ticker, Uuid::default())?)
            .await?;

        let temporal = ctx
            .client
            .fetch(&format!("https://api2.blaseball.com/temporal"))
            .await?;
        ctx.db
            .save(&temporal.to_chron(EntityKind::Temporal, Uuid::default())?)
            .await?;

        // todo: should we move this elseworker
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

        Ok(())
    }
}

pub async fn get_and_update_sim(ctx: &mut WorkerContext) -> anyhow::Result<()> {
    let sim = ctx
        .client
        .fetch(&format!("https://api2.blaseball.com/sim"))
        .await?;
    ctx.db
        .save(&sim.to_chron(EntityKind::Sim, Uuid::default())?)
        .await?;

    let sim: SimData = sim.parse()?;
    ctx.update_state(super::SimState {
        season: sim.sim_data.current_season_id,
        day: sim.sim_data.current_day,
    });
    Ok(())
}

#[derive(Deserialize)]
pub struct SimData {
    #[serde(rename = "simData")]
    pub sim_data: SimDataInner,
}

#[derive(Deserialize)]
pub struct SimDataInner {
    #[serde(rename = "currentSeasonId")]
    pub current_season_id: Uuid,
    #[serde(rename = "currentDay")]
    pub current_day: i32,
}
