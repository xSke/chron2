use std::{collections::HashSet, time::Duration};

use async_trait::async_trait;
use chron_db::{models::EntityKind, NewObject};
use futures::{stream, StreamExt};
use serde::Deserialize;
use tracing::error;
use uuid::Uuid;

use crate::util::get_uuid;

use super::{IntervalWorker, WorkerContext};

pub struct PollAllGames;

#[async_trait]
impl IntervalWorker for PollAllGames {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let (season, day) = ctx.season_day();
        let resp = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/games",
                season
            ))
            .await?;

        // parse all games first
        let games = resp
            .parse::<Vec<serde_json::Value>>()?
            .into_iter()
            .map(|val| serde_json::from_value::<Game>(val.clone()).map(|x| (val, x)))
            .flatten()
            .collect::<Vec<_>>();

        // subscribe to pending games before saving everything
        for (_, game) in &games {
            if !game.complete && game.day < day + 15 {
                ctx.pusher
                    .subscribe(format!("game-feed-{}", game.id))
                    .await?;
            }
        }

        // then save all games
        for (game_value, game) in games {
            ctx.db
                .save(&NewObject {
                    kind: EntityKind::Game,
                    entity_id: game.id,
                    data: game_value,
                    request_time: resp.request_time(),
                    timestamp: resp.timestamp(),
                })
                .await?;
        }

        Ok(())
    }
}

pub struct PollLiveGames {
    finished_games: HashSet<Uuid>,
    i: u32,
}

impl PollLiveGames {
    pub fn new() -> PollLiveGames {
        PollLiveGames {
            finished_games: HashSet::new(),
            i: 0,
        }
    }
}

async fn poll_single_game(
    ctx: WorkerContext,
    game_id: Uuid,
    poll_extra: bool,
) -> anyhow::Result<Game> {
    let (season, _) = ctx.season_day();

    let game = ctx
        .client
        .fetch(&format!(
            "https://api2.blaseball.com/seasons/{}/games/{}",
            season, game_id
        ))
        .await?;

    // remove the cache buster entry that just gets in our way
    let mut game_value = game.parse::<serde_json::Value>()?;
    if let Some(inner) = game_value.as_object_mut() {
        inner.remove("fetchedAt");
    }
    let game_struct: Game = serde_json::from_value(game_value.clone())?;

    ctx.db
        .save(&NewObject {
            kind: EntityKind::Game,
            entity_id: game_id,
            request_time: game.request_time(),
            timestamp: game.timestamp(),
            data: game_value,
        })
        .await?;

    if poll_extra {
        // todo: do we really need to poll these often
        let box_score = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/games/{}/boxScore",
                season, game_id
            ))
            .await?;
        ctx.db
            .save(&box_score.to_chron(EntityKind::BoxScore, game_id)?)
            .await?;

        save_outcomes(&ctx, season, game_id).await?;
    }

    Ok(game_struct)
}

#[async_trait]
impl IntervalWorker for PollLiveGames {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(8))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let (season, _) = ctx.season_day();
        let resp = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/schedule/{}/live",
                season
            ))
            .await?;

        let game_ids = resp.parse::<LiveGames>()?.game_ids;
        for game_id in &game_ids {
            ctx.pusher
                .subscribe(format!("game-feed-{}", game_id))
                .await?;
        }

        let poll_extra = self.i % 2 == 0;

        let games = stream::iter(
            game_ids
                .into_iter()
                .filter(|x| !self.finished_games.contains(x)),
        )
        .map(|game_id| poll_single_game(ctx.clone(), game_id, poll_extra))
        .buffer_unordered(2)
        .filter_map(|x| async { x.map_err(|e| error!("{}", e)).ok() })
        .collect::<Vec<_>>()
        .await;

        if poll_extra {
            // only mark a game as properly finished if we've polled extra post-completion
            self.finished_games
                .extend(games.iter().filter(|g| g.complete).map(|x| x.id));
        }

        self.i += 1;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct Game {
    id: Uuid,
    day: i32,
    complete: bool,
}

#[derive(Debug, Deserialize)]
struct LiveGames {
    #[serde(rename = "gameIds")]
    game_ids: Vec<Uuid>,
}

pub struct PollAllGameOutcomes;

#[async_trait]
impl IntervalWorker for PollAllGameOutcomes {
    fn interval() -> tokio::time::Interval {
        // do this very infrequently
        tokio::time::interval(Duration::from_secs(60 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let (season, _) = ctx.season_day();
        let games = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/games",
                season
            ))
            .await?
            .parse::<Vec<Game>>()?;

        for game in games {
            if game.complete {
                save_outcomes(&ctx, season, game.id).await?;
            }
        }

        Ok(())
    }
}

async fn save_outcomes(ctx: &WorkerContext, season: Uuid, game_id: Uuid) -> anyhow::Result<()> {
    let resp = ctx
        .client
        .fetch(&format!(
            "https://api2.blaseball.com/seasons/{}/games/{}/outcomes",
            season, game_id
        ))
        .await?;

    let outcome_values = resp.parse::<Vec<serde_json::Value>>()?;

    // don't bother saving empty arrays all the time
    if outcome_values.len() > 0 {
        ctx.db
            .save(&resp.to_chron(EntityKind::GameOutcomes, game_id)?)
            .await?;
    }

    for outcome in outcome_values {
        if let Some(id) = get_uuid(&outcome, "id") {
            ctx.db
                .save(&NewObject {
                    kind: EntityKind::GameOutcome,
                    entity_id: id,
                    data: outcome,
                    timestamp: resp.timestamp(),
                    request_time: resp.request_time(),
                })
                .await?;
        }
    }

    Ok(())
}
