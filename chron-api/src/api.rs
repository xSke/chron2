use axum::{extract::{State, Query}, Json};
use chron_db::{models::{PusherEvent, GameEvent, EntityKind, EntityVersion}, queries::SortOrder};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{AppState, AppError};

#[derive(Serialize)]
pub struct WrappedResponse<T: Serialize> {
    items: Vec<T>
}

#[derive(Deserialize)]
pub struct GetGameEventsQuery {
    game_id: Option<Uuid>,
    search: Option<String>,
    before: Option<OffsetDateTime>,
    after: Option<OffsetDateTime>,
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
            before: q.before,
            after: q.after,
            count: 5000,
            order: q.order,
        })
        .await?;

    Ok(Json(events))
}

#[derive(Deserialize)]
pub struct GetEventsQuery {
    channel: Option<String>,
    // event: Option<String>,
    before: Option<OffsetDateTime>,
    after: Option<OffsetDateTime>,
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
            before: q.before,
            after: q.after,
            count: 5000,
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
        .get_entities(chron_db::queries::GetEntitiesQuery {
            kind: q.kind,
        })
        .await?;

    Ok(Json(WrappedResponse { items: events }))
}
