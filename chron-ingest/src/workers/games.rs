use std::{collections::HashSet, time::Duration};

use async_trait::async_trait;
use chron_db::{models::EntityKind, NewObject};
use futures::{stream, StreamExt};
use serde::Deserialize;
use tracing::error;
use uuid::Uuid;

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

        for game_value in resp.parse::<Vec<serde_json::Value>>()? {
            let game: Game = serde_json::from_value(game_value.clone())?;
            ctx.db
                .save(&NewObject {
                    kind: EntityKind::Game,
                    entity_id: game.id,
                    data: game_value,
                    request_time: resp.request_time(),
                    timestamp: resp.timestamp(),
                })
                .await?;

            if !game.complete && game.day < day + 2 {
                ctx.pusher
                    .subscribe(format!("game-feed-{}", game.id))
                    .await?;
            }
        }

        Ok(())
    }
}

pub struct PollLiveGames {
    finished_games: HashSet<Uuid>,
}

impl PollLiveGames {
    pub fn new() -> PollLiveGames {
        PollLiveGames {
            finished_games: HashSet::new(),
        }
    }
}

async fn poll_single_game(ctx: WorkerContext, game_id: Uuid) -> anyhow::Result<Game> {
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

    Ok(game_struct)
}

#[async_trait]
impl IntervalWorker for PollLiveGames {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(5))
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

        let games = stream::iter(
            game_ids
                .into_iter()
                .filter(|x| !self.finished_games.contains(x)),
        )
        .map(|game_id| poll_single_game(ctx.clone(), game_id))
        .buffer_unordered(4)
        .filter_map(|x| async { x.map_err(|e| error!("{}", e)).ok() })
        .collect::<Vec<_>>()
        .await;
        self.finished_games
            .extend(games.iter().filter(|g| g.complete).map(|x| x.id));

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
