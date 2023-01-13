use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};
use time::OffsetDateTime;
use uuid::Uuid;

#[repr(i16)]
#[derive(Debug, Clone, Copy, Type, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Game = 1,
    Player = 2,
    Team = 3,
    Flagsmith = 4,
    Ticker = 5,
    SeasonElections = 6,
    SeasonSchedule = 7,
    SeasonScheduleHourly = 8,
    Temporal = 9,
    Post = 10,
    GameBetData = 11,
    BoxScore = 12,
    Sim = 13,
    SeasonTournaments = 14, // todo: break this out later
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct PusherEvent {
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub channel: String,
    pub event: String,
    pub payload: Option<serde_json::Value>,

    #[serde(skip_serializing)]
    pub raw: String,
}

impl PusherEvent {
    pub fn new(
        timestamp: OffsetDateTime,
        channel: String,
        event: String,
        payload: Option<serde_json::Value>,
        raw: String,
    ) -> PusherEvent {
        PusherEvent {
            timestamp,
            channel,
            event,
            payload: payload,
            raw,
        }
    }
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct GameEvent {
    pub game_id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct EntityVersion {
    pub kind: EntityKind,
    pub entity_id: Uuid,

    #[serde(with = "time::serde::rfc3339")]
    pub valid_from: OffsetDateTime,

    // #[serde(with="time::serde::rfc3339")]
    // pub valid_to: Option<OffsetDateTime>,
    pub data: serde_json::Value,
}
