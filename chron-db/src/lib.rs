use std::{hash::Hasher, sync::Arc};

use chron_base::ChronConfig;
use dashmap::DashSet;
use models::{EntityKind, PusherEvent};
use siphasher::sip128::{Hasher128, SipHasher};
use sqlx::{postgres::PgPoolOptions, Executor, PgPool};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

pub mod models;
pub mod queries;

#[derive(Clone)]
pub struct ChronDb {
    pub pool: PgPool,
    saved_objects: Arc<DashSet<Uuid>>,
}

#[derive(Debug)]
pub struct NewObject {
    pub kind: EntityKind,
    pub entity_id: Uuid,
    pub data: serde_json::Value,
    pub timestamp: OffsetDateTime,
    pub request_time: Duration,
}

impl ChronDb {
    pub async fn new(config: &ChronConfig) -> anyhow::Result<ChronDb> {
        let opts = PgPoolOptions::new().max_connections(25);
        let pool = opts.connect(&config.database_uri).await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        let mut tx = pool.acquire().await?;
        tx.execute(include_str!("../migrations/functions.sql"))
            .await?;

        Ok(ChronDb {
            pool,
            saved_objects: Arc::new(DashSet::new()),
        })
    }

    pub async fn rebuild(&self, kind: EntityKind, entity_id: Uuid) -> anyhow::Result<()> {
        sqlx::query("select rebuild_entity($1::smallint, $2::uuid)")
            .bind(kind)
            .bind(entity_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn save(&self, obj: &NewObject) -> anyhow::Result<()> {
        let hash = self.save_object(&obj.data).await?;
        self.add_version(
            obj.kind,
            obj.entity_id,
            hash,
            obj.timestamp,
            obj.request_time,
        )
        .await?;

        Ok(())
    }

    pub async fn save_raw(&self, obj: &NewObject) -> anyhow::Result<()> {
        let hash = self.save_object(&obj.data).await?;
        sqlx::query("insert into observations (kind, entity_id, timestamp, request_time, hash) values ($1, $2, $3, $4, $5)")
            .bind(obj.kind)
            .bind(obj.entity_id)
            .bind(obj.timestamp)
            .bind(obj.request_time.as_seconds_f64())
            .bind(hash)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn save_pusher(&self, event: &PusherEvent) -> anyhow::Result<()> {
        sqlx::query("insert into events (timestamp, channel, event, payload, raw) values ($1, $2, $3, $4, $5)")
            .bind(event.timestamp)
            .bind(&event.channel)
            .bind(&event.event)
            .bind(&event.payload)
            .bind(&event.raw)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn save_object(&self, data: &serde_json::Value) -> anyhow::Result<Uuid> {
        let mut hasher = SipHasher::new();
        let body = serde_json::to_vec(data)?; // todo: is this deterministic
        hasher.write(&body);

        let hash = Uuid::from_u128(hasher.finish128().as_u128());

        // ok if we save double here
        if !self.saved_objects.contains(&hash) {
            sqlx::query("insert into objects (hash, data) values ($1, $2) on conflict do nothing")
                .bind(hash)
                .bind(data)
                .execute(&self.pool)
                .await?;
            self.saved_objects.insert(hash);
        }

        Ok(hash)
    }

    pub async fn save_game_event(&self, game_id: Uuid, data: serde_json::Value) -> anyhow::Result<()> {
        sqlx::query("insert into game_events (game_id, data, timestamp, search_tsv) select game_id, data, (data->>'displayTime')::timestamptz, to_tsvector(data->>'displayText') from (select $1 as game_id, $2 as data) as x")
            .bind(game_id)
            .bind(&data)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn add_version(
        &self,
        kind: EntityKind,
        entity_id: Uuid,
        hash: Uuid,
        timestamp: OffsetDateTime,
        request_time: Duration,
    ) -> anyhow::Result<()> {
        sqlx::query("select add_version($1, $2, $3, $4, $5)")
            .bind(kind)
            .bind(entity_id)
            .bind(hash)
            .bind(timestamp)
            .bind(request_time.as_seconds_f32())
            .execute(&self.pool)
            .await?;

        Ok(())
        /*let result: Option<UpsertVersionResponse> = sqlx::query_as("update latest_versions set seq = seq + 1, prev_seen = last_seen, hash = ?, last_seen = ? where kind = ? and entity_id = ? and hash != ? and last_seen < ? returning prev_seen, seq as new_seq")
            .bind(hash)
            .bind(timestamp)
            .bind(kind)
            .bind(entity_id)
            .bind(hash)
            .bind(timestamp)
            .fetch_optional(&self.pool).await?;

        if let Some(UpsertVersionResponse { prev_seen, new_seq }) = result {
            let old_seq = new_seq - 1;
            sqlx::query("insert into versions (kind, entity_id, seq, hash, valid_from) values (?, ?, ?, ?, ?)")
                .bind(kind)
                .bind(entity_id)
                .bind(new_seq)
                .bind(hash)
                .bind(timestamp)
                .execute(&self.pool)
                .await?;

            // close off the old version
            // todo: should we store last_seen somewhere?
            sqlx::query("update versions set valid_to = ?, last_seen = ? where kind = ? and entity_id = ? and seq = ?")
                .bind(timestamp)
                .bind(prev_seen)
                .bind(kind)
                .bind(entity_id)
                .bind(old_seq)
                .execute(&self.pool)
                .await?;

        } else {
            sqlx::query("update latest_versions set last_seen = max(last_seen, ?) where kind = ? and entity_id = ? and hash = ?")
                .bind(timestamp)
                .bind(kind)
                .bind(entity_id)
                .bind(hash)
                .execute(&self.pool)
                .await?;
        }
        sqlx::query("update latest_versions set latest_timestamp = ? where kind = ? and entity_id = ? and latest_hash = ? and latest_timestamp < ?")
            .bind(timestamp)
            .bind(kind)
            .bind(entity_id)
            .bind(hash)
            .bind(timestamp)
            .bind();*/
    }
}
