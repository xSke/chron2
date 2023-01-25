use std::sync::{Arc, RwLock};

use chron_base::load_config;
use chron_db::{
    models::{EntityKind, PusherEvent},
    ChronDb, NewObject,
};
use futures::{StreamExt, TryFutureExt};
use http::DataClient;
use pusher::{pusher_connect, PusherMessage};
use time::Duration;
use tracing::{error, info};
use uuid::Uuid;
use workers::{
    assets::PollAssets,
    elections::{PollBlessingPreferences, PollElections},
    games::{PollAllGameOutcomes, PollAllGames, PollLiveGames, PusherCatchup},
    posts::PollPosts,
    rosters::{PollActiveRosters, PollAllLeagueData},
    schedule::PollSchedule,
    sim::{PollSimData, SimData},
    IntervalWorker, SimState, WorkerContext,
};

pub mod asset;
mod http;
pub mod pusher;
pub mod util;
mod workers;

fn spawn<T: IntervalWorker + 'static>(mut ctx: WorkerContext, mut w: T) {
    tokio::spawn(async move {
        // let pin_w = pin_w;

        let mut interval = T::interval();
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            w.tick(&mut ctx)
                .unwrap_or_else(|e| {
                    let type_name = std::any::type_name::<T>().split("::").last().unwrap();
                    error!("error executing worker {}: {:?}", type_name, e);
                })
                .await;
        }
    });
}

async fn handle_pusher(ctx: &mut WorkerContext, msg: PusherMessage) -> anyhow::Result<()> {
    let pe = PusherEvent::new(msg.timestamp, msg.channel, msg.event, msg.payload, msg.data);
    ctx.db.save_pusher(&pe).await?;

    if let Some(payload) = pe.payload {
        if pe.event == "sim-data" {
            let sim_data = serde_json::from_value::<SimData>(payload.clone())?;
            ctx.update_state(SimState {
                season: sim_data.sim_data.current_season_id,
                day: sim_data.sim_data.current_day,
            });
            // todo: not saving this as chron object because it's a reduced object
        } else if pe.event == "temporal-message" {
            ctx.db
                .save(&NewObject {
                    kind: EntityKind::Temporal,
                    entity_id: Uuid::default(),
                    request_time: Duration::ZERO,
                    timestamp: pe.timestamp,
                    data: payload,
                })
                .await?;
        } else if pe.event == "game-data" {
            let game_id: Uuid = pe.channel.trim_start_matches("game-feed-").parse()?;

            for evt_value in serde_json::from_value::<Vec<serde_json::Value>>(payload)? {
                ctx.db.save_game_event(game_id, evt_value).await?;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = load_config()?;
    let (tx, rx) = pusher_connect("c481dafb635a60adffdd".to_string()).await?;

    let client = DataClient::new(&config.auth_cookie)?;
    let mut ctx = WorkerContext {
        client,
        db: ChronDb::new(&config).await?,
        pusher: tx,
        sim: Arc::new(RwLock::new(SimState {
            season: Uuid::default(),
            day: -1,
        })),
    };

    if config.crisis_mode {
        // doing this here is a bit nasty but eh
        workers::sim::get_and_update_sim(&mut ctx).await?;

        spawn(ctx.clone(), PollAllGames);
        spawn(ctx.clone(), PollLiveGames::new());
        // spawn(ctx.clone(), PollSchedule);
        spawn(ctx.clone(), PollPosts);
        spawn(ctx.clone(), PollSimData);
        spawn(ctx.clone(), PollActiveRosters);
        spawn(ctx.clone(), PollAllLeagueData);
        // spawn(ctx.clone(), PollElections);
        // spawn(ctx.clone(), PollBlessingPreferences);
        spawn(ctx.clone(), PollAssets);
        // spawn(ctx.clone(), PollAllGameOutcomes);
    } else {
        spawn(ctx.clone(), PusherCatchup);
    }

    let mut rx = Box::pin(rx);
    while let Some(msg) = rx.next().await {
        info!("received pusher message: {:?}", msg);
        if !msg.event.starts_with("pusher_internal:") {
            handle_pusher(&mut ctx, msg)
                .unwrap_or_else(|e| error!("{}", e))
                .await;
        }
    }

    Ok(())
}
