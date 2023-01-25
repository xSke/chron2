use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use async_trait::async_trait;
use chron_db::{models::EntityKind, NewObject};
use futures::{stream, StreamExt};
use serde::Deserialize;
use tokio::time::{interval, interval_at, Instant};
use tracing::error;
use uuid::Uuid;

use super::{IntervalWorker, WorkerContext};

pub struct PollActiveRosters;

#[async_trait]
impl IntervalWorker for PollActiveRosters {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(60 * 20))
    }

    async fn tick(&mut self, ctx: &mut super::WorkerContext) -> anyhow::Result<()> {
        let (season, day) = ctx.season_day();

        let resp = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/days/{}/teams",
                season, day
            ))
            .await?;
        let teams = resp.parse::<HashMap<String, Vec<serde_json::Value>>>()?;

        let mut player_ids = Vec::new();
        for team_value in teams.into_values().flatten() {
            let team = serde_json::from_value::<Team>(team_value.clone())?;
            ctx.db
                .save(&NewObject {
                    kind: EntityKind::Team,
                    entity_id: team.id,
                    request_time: resp.request_time(),
                    timestamp: resp.timestamp(),
                    data: team_value,
                })
                .await?;
            player_ids.extend(team.roster.iter().map(|x| x.id));
        }

        fetch_players(ctx, player_ids.into_iter()).await;

        Ok(())
    }
}

pub struct PollAllLeagueData;

#[async_trait]
impl IntervalWorker for PollAllLeagueData {
    fn interval() -> tokio::time::Interval {
        interval_at(Instant::now() + Duration::from_secs(60*10), Duration::from_secs(60 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        // first, fetch all the teams we know of
        let mut team_ids: HashSet<Uuid> =
            HashSet::from_iter(ctx.db.get_all_entity_ids(EntityKind::Team).await?);
        // team_ids.extend(
        //     include_str!("known_team_ids.txt")
        //         .split("\n")
        //         .flat_map(|x| x.trim().parse::<Uuid>()),
        // );
        let teams = fetch_teams(ctx, team_ids.iter().cloned()).await;

        // then, collect all the players we know of
        let mut player_ids: HashSet<Uuid> =
            HashSet::from_iter(ctx.db.get_all_entity_ids(EntityKind::Player).await?);
        // player_ids.extend(
        //     include_str!("known_player_ids.txt")
        //         .split("\n")
        //         .flat_map(|x| x.trim().parse::<Uuid>()),
        // );
        player_ids.extend(teams.iter().flat_map(|x| x.roster.iter().map(|x| x.id)));
        let players = fetch_players(ctx, player_ids.into_iter()).await;

        // if any of those players are on teams we haven't seen, fetch those too
        let new_team_ids: HashSet<Uuid> = players
            .iter()
            .flat_map(|x| x.team.as_ref().map(|x| x.id))
            .collect();
        fetch_teams(ctx, new_team_ids.difference(&team_ids).cloned()).await;

        // ...we could really go on this loop forever but we'll catch the next layer next time around

        Ok(())
    }
}

#[derive(Deserialize)]
struct Team {
    id: Uuid,
    roster: Vec<RosterPlayer>,
}

#[derive(Deserialize)]
struct RosterPlayer {
    id: Uuid,
}

async fn fetch_players(
    ctx: &mut WorkerContext,
    player_ids: impl Iterator<Item = Uuid>,
) -> Vec<PlayerData> {
    stream::iter(player_ids)
        .map(|player_id| fetch_player(ctx.clone(), player_id))
        .buffer_unordered(1)
        .filter_map(|x| async { x.map_err(|e| error!("{}", e)).ok() })
        .collect::<Vec<_>>()
        .await
}

async fn fetch_player(ctx: WorkerContext, player_id: Uuid) -> anyhow::Result<PlayerData> {
    let (season, day) = ctx.season_day();
    let resp = ctx
        .client
        .fetch(&format!(
            "https://api2.blaseball.com/seasons/{}/days/{}/players/{}",
            season, day, player_id
        ))
        .await?;
    ctx.db
        .save(&resp.to_chron(EntityKind::Player, player_id)?)
        .await?;

    tokio::time::sleep(Duration::from_millis(1000)).await;

    Ok(resp.parse()?)
}

async fn fetch_teams(ctx: &mut WorkerContext, team_ids: impl Iterator<Item = Uuid>) -> Vec<Team> {
    stream::iter(team_ids)
        .map(|team_id| fetch_team(ctx.clone(), team_id))
        .buffer_unordered(1)
        .filter_map(|x| async { x.map_err(|e| error!("{}", e)).ok() })
        .collect::<Vec<_>>()
        .await
}

async fn fetch_team(ctx: WorkerContext, team_id: Uuid) -> anyhow::Result<Team> {
    let (season, day) = ctx.season_day();
    let resp = ctx
        .client
        .fetch(&format!(
            "https://api2.blaseball.com/seasons/{}/days/{}/teams/{}",
            season, day, team_id
        ))
        .await?;
    ctx.db
        .save(&resp.to_chron(EntityKind::Team, team_id)?)
        .await?;

    tokio::time::sleep(Duration::from_millis(1000)).await;

    Ok(resp.parse()?)
}

#[derive(Deserialize)]
struct PlayerData {
    team: Option<PlayerTeamData>,
}

#[derive(Deserialize)]
struct PlayerTeamData {
    id: Uuid,
}
