use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chron_db::ChronDb;
use tokio::time::Interval;
use uuid::Uuid;

use crate::{http::DataClient, pusher::PusherHandle};

pub mod games;
pub mod posts;
pub mod rosters;
pub mod schedule;
pub mod sim;

#[derive(Clone)]
pub struct WorkerContext {
    pub pusher: PusherHandle,
    pub sim: Arc<RwLock<SimState>>,
    pub db: ChronDb,
    pub client: DataClient,
}

impl WorkerContext {
    pub fn season_day(&self) -> (Uuid, i32) {
        let s = self.sim.read().expect("should never be poisoned");
        (s.season.clone(), s.day)
    }

    pub fn update_state(&self, new_state: SimState) {
        let mut s = self.sim.write().expect("should never be poisoned");
        *s = new_state;
    }
}

#[async_trait]
pub trait IntervalWorker: Send + Sync {
    fn interval() -> Interval;

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()>;
}

pub struct SimState {
    pub season: Uuid,
    pub day: i32,
}
