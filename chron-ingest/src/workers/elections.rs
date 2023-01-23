use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use chron_db::{models::EntityKind, NewObject};
use futures::TryFutureExt;
use serde::{Deserialize, Serialize};
use tokio::time::interval;
use tracing::{error, info};
use uuid::Uuid;

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

pub struct PollBlessingPreferences;

#[async_trait]
impl IntervalWorker for PollBlessingPreferences {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(60 * 5))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let (season, day) = ctx.season_day();
        let teams = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/days/{}/teams",
                season, day
            ))
            .await?
            .parse::<HashMap<Uuid, Vec<TeamStub>>>()?;

        for team in teams.values().flatten() {
            get_and_save_team_election_data(ctx, team.id).await?;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        Ok(())
    }
}

async fn get_and_save_team_election_data(ctx: &WorkerContext, team_id: Uuid) -> anyhow::Result<()> {
    let (season, _) = ctx.season_day();

    info!("changing account team to {}", team_id);
    ctx.client.change_favorite_team(team_id).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = ctx
        .client
        .fetch(&format!(
            "https://api2.blaseball.com/seasons/{}/elections",
            season
        ))
        .await?;
    let data = response.parse::<ElectionData>()?;

    let groups = data
        .blessings
        .into_iter()
        .map(|group| TeamBlessingPreferencesInner {
            id: group.id,
            top_option_ids: group.favorite_team_top_option_ids,
        })
        .collect();

    let obj = TeamBlessingPreferences {
        // should always be the same team id we got in but you never know idk
        team_id: data.current_user_favorite_team.id,
        groups,
    };

    info!("team blessing preference: {:?}", obj);
    ctx.db
        .save(&NewObject {
            kind: EntityKind::TeamBlessingPreferences,
            entity_id: obj.team_id,
            data: serde_json::to_value(obj)?,
            request_time: response.request_time(),
            timestamp: response.timestamp(),
        })
        .await?;

    Ok(())
}

#[derive(Deserialize)]
struct ElectionData {
    blessings: Vec<BlessingData>,

    #[serde(rename = "currentUserFavoriteTeam")]
    current_user_favorite_team: UserFavoriteTeam,
}

#[derive(Deserialize)]
struct BlessingData {
    id: Uuid,

    #[serde(rename = "favoriteTeamTopOptionIds")]
    favorite_team_top_option_ids: Vec<Uuid>,
}

#[derive(Deserialize)]
struct UserFavoriteTeam {
    id: Uuid,
}

#[derive(Deserialize)]
struct TeamStub {
    id: Uuid,
}

#[derive(Serialize, Debug)]
struct TeamBlessingPreferences {
    team_id: Uuid,
    groups: Vec<TeamBlessingPreferencesInner>,
}

#[derive(Serialize, Debug)]
struct TeamBlessingPreferencesInner {
    id: Uuid,
    top_option_ids: Vec<Uuid>,
}
