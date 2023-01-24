use std::{fmt::Display, str::FromStr};

use base64::Engine;
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
    Tournament = 15,
    Asset = 16,
    ForbiddenBook = 17,
    TeamBlessingPreferences = 18,
    GameOutcomes = 19,
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
    pub valid_from: IsoDateTime,
    pub valid_to: Option<IsoDateTime>,
    pub data: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug, Clone, sqlx::Type)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct IsoDateTime(#[serde(with = "time::serde::rfc3339")] pub OffsetDateTime);

impl From<OffsetDateTime> for IsoDateTime {
    fn from(value: OffsetDateTime) -> Self {
        IsoDateTime(value)
    }
}

impl Into<OffsetDateTime> for IsoDateTime {
    fn into(self) -> OffsetDateTime {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct PageToken {
    pub entity_id: Uuid,
    pub timestamp: OffsetDateTime,
}

impl Display for PageToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = [0u8; 32];
        buf[0..16].copy_from_slice(&self.timestamp.unix_timestamp_nanos().to_le_bytes());
        buf[16..32].copy_from_slice(self.entity_id.as_bytes());

        let engine = base64::engine::general_purpose::URL_SAFE;
        f.write_str(&engine.encode(&buf))
    }
}

impl FromStr for PageToken {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let engine = base64::engine::general_purpose::URL_SAFE;
        let data = engine.decode(s)?;
        if data.len() != 32 {
            return Err(anyhow::anyhow!("invalid page token"));
        }

        let timestamp_nanos = i128::from_le_bytes(data[0..16].try_into().unwrap());
        let timestamp = OffsetDateTime::from_unix_timestamp_nanos(timestamp_nanos)?;
        let entity_id = Uuid::from_slice(&data[16..32])?;

        Ok(PageToken {
            entity_id,
            timestamp,
        })
    }
}

impl Serialize for PageToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for PageToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?;
        PageToken::from_str(&str).map_err(|_| serde::de::Error::custom("invalid page token"))
    }
}

pub trait HasPageToken {
    fn page_token(&self) -> PageToken;
}

impl HasPageToken for EntityVersion {
    fn page_token(&self) -> PageToken {
        PageToken {
            entity_id: self.entity_id,
            timestamp: self.valid_from.0,
        }
    }
}

impl HasPageToken for GameEvent {
    fn page_token(&self) -> PageToken {
        PageToken {
            entity_id: Uuid::default(),
            timestamp: self.timestamp,
        }
    }
}

impl HasPageToken for PusherEvent {
    fn page_token(&self) -> PageToken {
        PageToken {
            entity_id: Uuid::default(),
            timestamp: self.timestamp,
        }
    }
}
