use sea_query::{Expr, Iden, PostgresQueryBuilder, Query};
use sea_query_binder::SqlxBinder;
use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    models::{EntityKind, EntityVersion, GameEvent, PusherEvent},
    ChronDb,
};

// i hate this and i wanna throw it out
#[derive(Iden)]
enum Idens {
    Channel,
    Data,
    EntityId,
    Event,
    Events,
    GameEvents,
    GameId,
    Hash,
    Kind,
    Objects,
    Payload,
    Raw,
    SearchTsv,
    Timestamp,
    ValidFrom,
    // ValidTo,
    Versions,
}

#[derive(Deserialize, Debug)]
pub enum SortOrder {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

impl Default for SortOrder {
    fn default() -> Self {
        SortOrder::Asc
    }
}

pub struct GetGameEventsQuery {
    pub game_id: Option<Uuid>,
    pub search: Option<String>,
    pub before: Option<OffsetDateTime>,
    pub after: Option<OffsetDateTime>,
    pub count: u64,
    pub order: SortOrder,
}

pub struct GetEventsQuery {
    pub channel: Option<String>,
    // pub event: Option<String>,
    pub before: Option<OffsetDateTime>,
    pub after: Option<OffsetDateTime>,
    pub count: u64,
    pub order: SortOrder,
}

pub struct GetEntitiesQuery {
    pub kind: EntityKind,
}

fn get_order(order: SortOrder) -> sea_query::Order {
    match order {
        SortOrder::Asc => sea_query::Order::Asc,
        SortOrder::Desc => sea_query::Order::Desc,
    }
}

pub struct GetVersionsQuery {
    pub kind: EntityKind,
    pub id: Vec<Uuid>,
    pub before: Option<OffsetDateTime>,
    pub after: Option<OffsetDateTime>,
    pub count: u64,
    pub order: SortOrder,
    // todo: page token
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
            .order_by(Idens::Timestamp, get_order(q.order))
            .limit(q.count)
            .to_owned();

        if let Some(game_id) = q.game_id {
            qq = qq
                .and_where(Expr::col(Idens::GameId).eq(game_id))
                .to_owned();
        }

        if let Some(search) = q.search {
            qq = qq
                .and_where(Expr::cust_with_exprs(
                    "$1 @@ to_tsquery($2)",
                    [Expr::col(Idens::SearchTsv).into(), Expr::val(search).into()],
                ))
                .to_owned();
        }

        if let Some(before) = q.before {
            qq = qq
                .and_where(Expr::col(Idens::Timestamp).lte(before))
                .to_owned();
        }

        if let Some(after) = q.after {
            qq = qq
                .and_where(Expr::col(Idens::Timestamp).gte(after))
                .to_owned();
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        let res = sqlx::query_as_with(&q, vals).fetch_all(&self.pool).await?;

        Ok(res)
    }

    pub async fn get_events(&self, q: GetEventsQuery) -> anyhow::Result<Vec<PusherEvent>> {
        let mut qq = Query::select()
            .columns([
                Idens::Channel,
                Idens::Event,
                Idens::Timestamp,
                Idens::Payload,
                Idens::Raw,
            ])
            .from(Idens::Events)
            .order_by(Idens::Timestamp, get_order(q.order))
            .limit(q.count)
            .to_owned();

        if let Some(channel) = q.channel {
            qq = qq
                .and_where(Expr::col(Idens::Channel).eq(channel))
                .to_owned();
        }

        // if let Some(event) = q.event {
        //     qq.and_where(Expr::col(EventsIden::Channel).eq(event));
        // }

        if let Some(before) = q.before {
            qq = qq
                .and_where(Expr::col(Idens::Timestamp).lte(before))
                .to_owned();
        }

        if let Some(after) = q.after {
            qq = qq
                .and_where(Expr::col(Idens::Timestamp).gte(after))
                .to_owned();
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        let res = sqlx::query_as_with(&q, vals).fetch_all(&self.pool).await?;

        Ok(res)
    }

    pub async fn get_entities(&self, q: GetEntitiesQuery) -> anyhow::Result<Vec<EntityVersion>> {
        // don't really feel like sea-query for this
        Ok(sqlx::query_as(
            "select * from latest_versions inner join objects using (hash) where kind = $1",
        )
        .bind(q.kind)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_versions(&self, q: GetVersionsQuery) -> anyhow::Result<Vec<EntityVersion>> {
        let mut qq = Query::select()
            .expr(Expr::table_asterisk(Idens::Versions))
            .expr(Expr::col(Idens::Data))
            .from(Idens::Versions)
            .order_by(Idens::ValidFrom, get_order(q.order))
            .limit(q.count)
            .inner_join(
                Idens::Objects,
                Expr::col((Idens::Versions, Idens::Hash)).equals((Idens::Objects, Idens::Hash)),
            )
            .and_where(Expr::col(Idens::Kind).eq(q.kind as i32))
            .to_owned();

        if !q.id.is_empty() {
            qq = qq
                .and_where(Expr::col(Idens::EntityId).is_in(q.id))
                .to_owned();
        }

        if let Some(before) = q.before {
            qq = qq
                .and_where(Expr::col(Idens::ValidFrom).lte(before))
                .to_owned();
        }

        if let Some(after) = q.after {
            qq = qq
                .and_where(Expr::col(Idens::ValidFrom).gte(after))
                .to_owned();
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        dbg!(&q, &vals);
        let res = sqlx::query_as_with(&q, vals).fetch_all(&self.pool).await?;
        Ok(res)
    }
}
