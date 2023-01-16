use sea_query::{Expr, Iden, PostgresQueryBuilder, Query, SimpleExpr};
use sea_query_binder::SqlxBinder;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    models::{EntityKind, EntityVersion, GameEvent, HasPageToken, PageToken, PusherEvent},
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
    ValidTo,
    Versions,
}

#[derive(Deserialize, Debug, Clone, Copy)]
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

#[derive(Serialize, Debug)]
pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub next_page: Option<PageToken>,
}

pub struct GetGameEventsQuery {
    pub game_id: Option<Uuid>,
    pub search: Option<String>,
    pub before: Option<OffsetDateTime>,
    pub after: Option<OffsetDateTime>,
    pub count: u64,
    pub order: SortOrder,
    pub page: Option<PageToken>,
}

pub struct GetEventsQuery {
    pub channel: Option<String>,
    pub before: Option<OffsetDateTime>,
    pub after: Option<OffsetDateTime>,
    pub count: u64,
    pub order: SortOrder,
    pub page: Option<PageToken>,
}

pub struct GetEntitiesQuery {
    pub kind: EntityKind,
    pub at: Option<OffsetDateTime>,
    pub id: Vec<Uuid>,
    pub order: SortOrder,
    pub page: Option<PageToken>,
}

pub struct GetVersionsQuery {
    pub kind: EntityKind,
    pub id: Vec<Uuid>,
    pub before: Option<OffsetDateTime>,
    pub after: Option<OffsetDateTime>,
    pub count: u64,
    pub order: SortOrder,
    pub page: Option<PageToken>,
}

impl ChronDb {
    pub async fn get_all_entity_ids(&self, kind: EntityKind) -> anyhow::Result<Vec<Uuid>> {
        let ids = sqlx::query_scalar("select entity_id from latest_versions where kind = $1")
            .bind(kind)
            .fetch_all(&self.pool)
            .await?;
        Ok(ids)
    }

    pub async fn get_game_events(
        &self,
        q: GetGameEventsQuery,
    ) -> anyhow::Result<PaginatedResult<GameEvent>> {
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

        if let Some(page) = q.page {
            qq = qq
                .and_where(paginate(q.order, Idens::Timestamp, None, page))
                .to_owned();
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        let res = sqlx::query_as_with(&q, vals).fetch_all(&self.pool).await?;

        Ok(with_page_token(res))
    }

    pub async fn get_events(
        &self,
        q: GetEventsQuery,
    ) -> anyhow::Result<PaginatedResult<PusherEvent>> {
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

        if let Some(page) = q.page {
            qq = qq
                .and_where(paginate(q.order, Idens::Timestamp, None, page))
                .to_owned();
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        let res = sqlx::query_as_with(&q, vals).fetch_all(&self.pool).await?;

        Ok(with_page_token(res))
    }

    pub async fn get_entities(
        &self,
        q: GetEntitiesQuery,
    ) -> anyhow::Result<PaginatedResult<EntityVersion>> {
        let mut qq = Query::select()
            .expr(Expr::table_asterisk(Idens::Versions))
            .expr(Expr::col(Idens::Data))
            .from(Idens::Versions)
            .inner_join(
                Idens::Objects,
                Expr::col((Idens::Versions, Idens::Hash)).equals((Idens::Objects, Idens::Hash)),
            )
            .order_by_columns([
                (Idens::ValidFrom, get_order(q.order)),
                (Idens::EntityId, get_order(q.order)),
            ])
            .and_where(Expr::col(Idens::Kind).eq(q.kind as i32))
            .to_owned();

        if !q.id.is_empty() {
            qq = qq
                .and_where(Expr::col(Idens::EntityId).is_in(q.id))
                .to_owned();
        }

        if let Some(at) = q.at {
            qq = qq
                .and_where(Expr::val(at).gte(Expr::col(Idens::ValidFrom)))
                .and_where(Expr::val(at).lt(Expr::cust_with_expr(
                    "coalesce($1, 'infinity')",
                    Expr::col(Idens::ValidTo),
                )))
                .to_owned();
        } else {
            qq = qq.and_where(Expr::col(Idens::ValidTo).is_null()).to_owned();
        }

        if let Some(page) = q.page {
            qq = qq
                .and_where(paginate(
                    q.order,
                    Idens::ValidFrom,
                    Some(Idens::EntityId),
                    page,
                ))
                .to_owned();
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        let res = sqlx::query_as_with(&q, vals).fetch_all(&self.pool).await?;
        Ok(with_page_token(res))
    }

    pub async fn get_versions(
        &self,
        q: GetVersionsQuery,
    ) -> anyhow::Result<PaginatedResult<EntityVersion>> {
        let mut qq = Query::select()
            .expr(Expr::table_asterisk(Idens::Versions))
            .expr(Expr::col(Idens::Data))
            .from(Idens::Versions)
            .order_by_columns([
                (Idens::ValidFrom, get_order(q.order)),
                (Idens::EntityId, get_order(q.order)),
            ])
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

        if let Some(page) = q.page {
            qq = qq
                .and_where(paginate(
                    q.order,
                    Idens::ValidFrom,
                    Some(Idens::EntityId),
                    page,
                ))
                .to_owned();
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        let res = sqlx::query_as_with(&q, vals).fetch_all(&self.pool).await?;
        Ok(with_page_token(res))
    }
}

fn paginate(
    order: SortOrder,
    timestamp_col: Idens,
    id_col: Option<Idens>,
    page_token: PageToken,
) -> SimpleExpr {
    let (ls, rs) = if let Some(id_col) = id_col {
        let ls = Expr::tuple([Expr::col(timestamp_col).into(), Expr::col(id_col).into()]);
        let rs = Expr::tuple([
            Expr::value(page_token.timestamp),
            Expr::value(page_token.entity_id),
        ]);
        (ls, rs)
    } else {
        (Expr::col(timestamp_col), Expr::val(page_token.timestamp))
    };

    match order {
        SortOrder::Asc => ls.gt(rs),
        SortOrder::Desc => ls.lt(rs),
    }
}

fn with_page_token<T: HasPageToken>(items: Vec<T>) -> PaginatedResult<T> {
    let pt = items.last().map(|e| e.page_token());
    PaginatedResult {
        items,
        next_page: pt,
    }
}

fn get_order(order: SortOrder) -> sea_query::Order {
    match order {
        SortOrder::Asc => sea_query::Order::Asc,
        SortOrder::Desc => sea_query::Order::Desc,
    }
}
