use core::fmt;
use std::{fmt::Display, marker::PhantomData, str::FromStr};

use axum::{
    extract::{Query, State},
    Json,
};
use chron_db::{
    models::{EntityKind, EntityVersion, GameEvent, PusherEvent},
    queries::SortOrder,
};
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize,
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{AppError, AppState};

#[derive(Serialize)]
pub struct WrappedResponse<T: Serialize> {
    items: Vec<T>,
}

#[derive(Deserialize)]
pub struct GetGameEventsQuery {
    game_id: Option<Uuid>,
    search: Option<String>,
    before: Option<IsoDateTime>,
    after: Option<IsoDateTime>,
    count: Option<u64>,
    #[serde(default)]
    order: SortOrder,
}

pub async fn get_game_events(
    State(ctx): State<AppState>,
    Query(q): Query<GetGameEventsQuery>,
) -> Result<Json<Vec<GameEvent>>, AppError> {
    let events = ctx
        .db
        .get_game_events(chron_db::queries::GetGameEventsQuery {
            game_id: q.game_id,
            search: q.search,
            before: q.before.map(|x| x.0),
            after: q.after.map(|x| x.0),
            count: q.count.unwrap_or(5000).min(5000),
            order: q.order,
        })
        .await?;

    Ok(Json(events))
}

#[derive(Deserialize)]
pub struct GetEventsQuery {
    channel: Option<String>,
    // event: Option<String>,
    before: Option<IsoDateTime>,
    after: Option<IsoDateTime>,
    count: Option<u64>,
    #[serde(default)]
    order: SortOrder,
}

pub async fn get_events(
    State(ctx): State<AppState>,
    Query(q): Query<GetEventsQuery>,
) -> Result<Json<Vec<PusherEvent>>, AppError> {
    let events = ctx
        .db
        .get_events(chron_db::queries::GetEventsQuery {
            channel: q.channel,
            // event: q.event,
            before: q.before.map(|x| x.0),
            after: q.after.map(|x| x.0),
            count: q.count.unwrap_or(5000).min(5000),
            order: q.order,
        })
        .await?;

    Ok(Json(events))
}

#[derive(Deserialize)]
pub struct GetEntitiesQuery {
    kind: EntityKind,
}

pub async fn get_entities(
    State(ctx): State<AppState>,
    Query(q): Query<GetEntitiesQuery>,
) -> Result<Json<WrappedResponse<EntityVersion>>, AppError> {
    let events = ctx
        .db
        .get_entities(chron_db::queries::GetEntitiesQuery { kind: q.kind })
        .await?;

    Ok(Json(WrappedResponse { items: events }))
}

#[derive(Deserialize, Debug)]
pub struct GetVersionsQuery {
    pub kind: EntityKind,

    #[serde(deserialize_with = "comma_separated", default)]
    pub id: Vec<Uuid>,
    pub before: Option<IsoDateTime>,
    pub after: Option<IsoDateTime>,
    pub count: Option<u64>,
    #[serde(default)]
    pub order: SortOrder,
    // todo: page token
}

pub async fn get_versions(
    State(ctx): State<AppState>,
    Query(q): Query<GetVersionsQuery>,
) -> Result<Json<WrappedResponse<EntityVersion>>, AppError> {
    println!("{:?}", &q);
    let events = ctx
        .db
        .get_versions(chron_db::queries::GetVersionsQuery {
            kind: q.kind,
            id: q.id,
            before: q.before.map(|x| x.0),
            after: q.after.map(|x| x.0),
            count: q.count.unwrap_or(1000).min(1000),
            order: q.order,
        })
        .await?;

    Ok(Json(WrappedResponse { items: events }))
}

fn comma_separated<'de, V, T, D>(deserializer: D) -> Result<V, D::Error>
where
    V: FromIterator<T>,
    T: FromStr,
    T::Err: Display,
    D: Deserializer<'de>,
{
    struct CommaSeparated<V, T>(PhantomData<V>, PhantomData<T>);

    impl<'de, V, T> Visitor<'de> for CommaSeparated<V, T>
    where
        V: FromIterator<T>,
        T: FromStr,
        T::Err: Display,
    {
        type Value = V;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string containing comma-separated elements")
        }

        fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let iter = s.split(",").map(FromStr::from_str);
            Result::from_iter(iter).map_err(de::Error::custom)
        }
    }

    let visitor = CommaSeparated(PhantomData, PhantomData);
    deserializer.deserialize_str(visitor)
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(transparent)]
pub struct IsoDateTime(#[serde(with = "time::serde::rfc3339")] OffsetDateTime);

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
