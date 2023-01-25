use std::hash::Hasher;

use config::Config;
use serde::Deserialize;
use siphasher::sip128::{Hasher128, SipHasher};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct ChronConfig {
    pub database_uri: String,
    pub auth_cookie: String,
    #[serde(default)]
    pub crisis_mode: bool,
}

pub fn load_config() -> anyhow::Result<ChronConfig> {
    // maybe we shouldn't do this here idk
    // tracing_subscriber::fmt::init();
    tracing_subscriber::fmt().compact().without_time().init();

    let settings = Config::builder()
        .add_source(config::File::with_name("config"))
        .add_source(config::Environment::with_prefix("CHRON"))
        .build()?
        .try_deserialize()?;
    Ok(settings)
}

pub fn uuid_hash(data: &[u8]) -> Uuid {
    let mut hasher = SipHasher::new();
    hasher.write(&data);

    Uuid::from_u128(hasher.finish128().as_u128())
}
