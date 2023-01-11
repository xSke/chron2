use std::str::FromStr;

use base64::Engine;
use chron_base::load_config;
use chron_db::{
    models::{EntityKind, PusherEvent},
    ChronDb, NewObject,
};
use flate2::bufread::GzDecoder;
use futures::{stream, StreamExt};
use serde::Deserialize;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    FromRow, Row,
};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

#[derive(Deserialize)]
struct WrappedPayload {
    message: String,
}

pub fn decode_payload(payload: &str) -> anyhow::Result<serde_json::Value> {
    let inner = serde_json::from_str::<WrappedPayload>(payload)?;

    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(&inner.message) {
        let gzip = GzDecoder::new(&decoded[..]);
        return Ok(serde_json::from_reader(gzip)?);
    } else if let Ok(value) = serde_json::from_str::<serde_json::Value>(&inner.message) {
        Ok(value)
    } else {
        Ok(serde_json::from_str::<serde_json::Value>(payload)?)
    }
}

fn parse_fetch(row: FetchRow) -> anyhow::Result<Vec<NewObject>> {
    if row.url.contains("ateb-api2") {
        return Ok(vec![]);
    }
    if row.url.contains("/players/") {
        let player_id = row.url.split("/").last().unwrap().parse::<Uuid>()?;
        let ch = row.to_chron(EntityKind::Player, player_id, row.parse()?);
        Ok(vec![ch])
        // dbg!(ch);
    } else if row.url.contains("/games/") {
        let game_id = row
            .url
            .split("/games/")
            .last()
            .unwrap()
            .split("/")
            .next()
            .unwrap()
            .parse::<Uuid>()?;
        // dbg!(game_id);
        if row.url.ends_with("/boxScore") {
            let ch = row.to_chron(EntityKind::BoxScore, game_id, row.parse()?);
            Ok(vec![ch])
        } else {
            let mut data = row.parse()?;
            if let Some(obj) = data.as_object_mut() {
                obj.remove("fetchedAt");
            }

            let ch = row.to_chron(EntityKind::Game, game_id, data);
            Ok(vec![ch])
        }
    } else if row.url.contains("/feed") {
        let data = row.parse()?;

        let mut posts = vec![];
        for post in data.get("posts").unwrap().as_array().unwrap() {
            let id: Uuid = post.get("id").unwrap().as_str().unwrap().parse()?;
            posts.push(row.to_chron(EntityKind::Post, id, post.clone()));
        }
        Ok(posts)
    } else if row.url.ends_with("/sim") {
        let ch = row.to_chron(EntityKind::Sim, Uuid::default(), row.parse()?);
        Ok(vec![ch])
    } else if row.url.ends_with("/temporal") {
        let ch = row.to_chron(EntityKind::Temporal, Uuid::default(), row.parse()?);
        Ok(vec![ch])
    } else if row.url.ends_with("/flagsmith") {
        let ch = row.to_chron(EntityKind::Flagsmith, Uuid::default(), row.parse()?);
        Ok(vec![ch])
    } else if row.url.contains("/user-ticker/") {
        let ch = row.to_chron(EntityKind::Ticker, Uuid::default(), row.parse()?);
        Ok(vec![ch])
    } else if row.url.ends_with("/elections") {
        let season_id = row
            .url
            .split("/seasons/")
            .last()
            .unwrap()
            .split("/")
            .next()
            .unwrap()
            .parse::<Uuid>()?;
        let ch = row.to_chron(EntityKind::SeasonElections, season_id, row.parse()?);
        Ok(vec![ch])
    } else if row.url.ends_with("/hourly") {
        let data = row.parse()?;
        Ok(data
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|x| x.get("betDatas").unwrap().as_array().unwrap())
            .map(|bd| {
                let game_id: Uuid = bd.get("gameId").unwrap().as_str().unwrap().parse().unwrap();
                row.to_chron(EntityKind::GameBetData, game_id, bd.clone())
            })
            .collect())
    } else if row.url.contains("/schedule/") {
        let season_id = row
            .url
            .split("/schedule/")
            .last()
            .unwrap()
            .split("/")
            .next()
            .unwrap()
            .parse::<Uuid>()?;
        let ch = row.to_chron(EntityKind::SeasonSchedule, season_id, row.parse()?);
        Ok(vec![ch])
    } else if row.url.ends_with("/live") || row.url.contains("/schedule/") {
        // ignore
        Ok(vec![])
    } else if row.url.ends_with("/games") {
        let data = row.parse()?;

        Ok(data
            .as_array()
            .unwrap()
            .iter()
            .map(|game| {
                let id: Uuid = game.get("id").unwrap().as_str().unwrap().parse().unwrap();
                row.to_chron(EntityKind::Game, id, game.clone())
            })
            .collect())
    } else if row.url.ends_with("/teams") {
        let data = row.parse()?;

        Ok(data
            .as_object()
            .unwrap()
            .values()
            .flat_map(|x| x.as_array().unwrap())
            .map(|team| {
                let id: Uuid = team.get("id").unwrap().as_str().unwrap().parse().unwrap();
                row.to_chron(EntityKind::Team, id, team.clone())
            })
            .collect())
    } else {
        println!("{}", row.url);
        Ok(vec![])
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = load_config()?;
    let db = ChronDb::new(&config).await?;

    let sqlite_path = std::env::args().nth(1).unwrap();

    let options =
        SqliteConnectOptions::from_str(&format!("sqlite://{}", sqlite_path))?.read_only(true);
    let qd = SqlitePoolOptions::new().connect_with(options).await?;

    println!("starting import");
    let stream = sqlx::query("select * from fetches").fetch(&qd);
    stream
        .map(|row| {
            let row = row.unwrap();
            let row = FetchRow {
                data: row.get("data"),
                // etag: row.get("etag"),
                // server_date: row.get("server_date"),
                url: row.get("url"),
                status_code: row.get("status_code"),
                // was_cached: row.get("was_cached"),
                timestamp_after: row
                    .try_get::<f64, _>("timestamp_after")
                    .or_else(|_| row.try_get::<i64, _>("timestamp_after").map(|x| x as f64))
                    .unwrap(),
                timestamp_before: row
                    .try_get::<f64, _>("timestamp_before")
                    .or_else(|_| row.try_get::<i64, _>("timestamp_before").map(|x| x as f64))
                    .unwrap(),
            };
            if row.status_code.unwrap_or_default() == 500
                || row.status_code.unwrap_or_default() == 503
                || row.status_code.unwrap_or_default() == 401
            {
                return stream::iter(vec![]);
            }

            println!("{}", &row.url);
            stream::iter(parse_fetch(row).unwrap())
        })
        .flatten()
        .for_each_concurrent(16, |obj| {
            let db = db.clone();
            async move {
                if let Err(e) = db.save_raw(&obj).await {
                    dbg!(e);
                }
            }
        })
        .await;

    println!("importing events");
    let stream = sqlx::query_as::<_, EventRow>("select * from events").fetch(&qd);
    stream
        .map(|row| {
            let row = row.unwrap();
            let ts = OffsetDateTime::from_unix_timestamp_nanos(
                (row.timestamp * 1_000_000_000.0) as i128,
            )
            .unwrap();
            let payload = decode_payload(&row.payload).ok();

            println!("{} / {}", row.channel, row.event);

            PusherEvent::new(ts, row.channel, row.event, payload, row.payload)
        })
        .for_each_concurrent(16, |obj| {
            let db = db.clone();
            async move {
                if let Err(e) = db.save_pusher(&obj).await {
                    dbg!(e);
                }
            }
        })
        .await;

    Ok(())
}

struct FetchRow {
    url: String,
    timestamp_before: f64,
    timestamp_after: f64,
    // server_date: String,
    // etag: String,
    data: Vec<u8>,
    status_code: Option<i32>,
    // was_cached: Option<bool>,
}

impl FetchRow {
    fn to_chron(&self, kind: EntityKind, entity_id: Uuid, data: serde_json::Value) -> NewObject {
        let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            (self.timestamp_before * 1_000_000.0) as i128 * 1_000,
        )
        .unwrap();
        let request_time = Duration::microseconds(
            ((self.timestamp_after - self.timestamp_before) * 1_000_000.0) as i64,
        );

        NewObject {
            kind,
            entity_id,
            data,
            timestamp,
            request_time,
        }
    }

    fn parse(&self) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::from_slice(&self.data)?)
    }
}

#[derive(FromRow, Debug)]
struct EventRow {
    timestamp: f64,
    channel: String,
    event: String,
    payload: String,
}
