use sea_query::{Query, Expr, Iden, PostgresQueryBuilder};
use sea_query_binder::SqlxBinder;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{models::{EntityKind, GameEvent, PusherEvent, EntityVersion}, ChronDb};

// i hate this and i wanna throw it out
#[derive(Iden)]
enum Idens {
    GameEvents,
    Events,
    GameId,
    Timestamp,
    Data,
    SearchTsv,
    Channel,
    Event,
    Payload,
    Raw,
}

pub struct GetGameEventsQuery {
    pub game_id: Option<Uuid>,
    pub search: Option<String>,
    pub before: Option<OffsetDateTime>,
    pub after: Option<OffsetDateTime>,
}

pub struct GetEventsQuery {
    pub channel: Option<String>,
    // pub event: Option<String>,
    pub before: Option<OffsetDateTime>,
    pub after: Option<OffsetDateTime>,
}

pub struct GetEntitiesQuery {
    pub kind: EntityKind
}


impl ChronDb {
    pub async fn get_all_entity_ids(&self, kind: EntityKind) -> anyhow::Result<Vec<Uuid>> {
        let ids = sqlx::query_scalar("select entity_id from latest_versions where kind = $1")
            .bind(kind)
            .fetch_all(&self.pool)
            .await?;
        Ok(ids)
    }
    
    pub async fn get_game_events(&self, q: GetGameEventsQuery) -> anyhow::Result<Vec<GameEvent>> {
        let mut qq = Query::select()
            .columns([Idens::GameId, Idens::Timestamp, Idens::Data])
            .from(Idens::GameEvents)
            .order_by(Idens::Timestamp, sea_query::Order::Asc)
            .to_owned();

        if let Some(game_id) = q.game_id {
            qq.and_where(Expr::col(Idens::GameId).eq(game_id));
        }

        if let Some(search) = q.search {
            qq.and_where(Expr::cust_with_exprs("$1 @@ to_tsquery($2)", [Expr::col(Idens::SearchTsv).into(), Expr::val(search).into()]));
        }

        if let Some(before) = q.before {
            qq.and_where(Expr::col(Idens::Timestamp).lte(before));
        }

        if let Some(after) = q.after {
            qq.and_where(Expr::col(Idens::Timestamp).gte(after));
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        let res = sqlx::query_as_with(&q, vals)
        .fetch_all(&self.pool)
        .await?;

        Ok(res)
    }

    pub async fn get_events(&self, q: GetEventsQuery) -> anyhow::Result<Vec<PusherEvent>> {
        let mut qq = Query::select()
            .columns([Idens::Channel, Idens::Event, Idens::Timestamp, Idens::Payload, Idens::Raw])
            .from(Idens::Events)
            .order_by(Idens::Timestamp, sea_query::Order::Asc)
            .to_owned();

        if let Some(channel) = q.channel {
            qq.and_where(Expr::col(Idens::Channel).eq(channel));
        }

        // if let Some(event) = q.event {
        //     qq.and_where(Expr::col(EventsIden::Channel).eq(event));
        // }

        if let Some(before) = q.before {
            qq.and_where(Expr::col(Idens::Timestamp).lte(before));
        }

        if let Some(after) = q.after {
            qq.and_where(Expr::col(Idens::Timestamp).gte(after));
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        let res = sqlx::query_as_with(&q, vals)
        .fetch_all(&self.pool)
        .await?;

        Ok(res)
    }

    pub async fn get_entities(&self, q: GetEntitiesQuery) -> anyhow::Result<Vec<EntityVersion>> {
        // don't really feel like sea-query for this
        Ok(sqlx::query_as("select * from latest_versions inner join objects using (hash) where kind = $1")
            .bind(q.kind)
            .fetch_all(&self.pool)
            .await?)
    }
}
