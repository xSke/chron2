use config::Config;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct ChronConfig {
    pub database_uri: String,
    pub auth_cookie: String,
}

pub fn load_config() -> anyhow::Result<ChronConfig> {
    // maybe we shouldn't do this here idk
    tracing_subscriber::fmt::init();

    let settings = Config::builder()
        .add_source(config::File::with_name("config"))
        .add_source(config::Environment::with_prefix("CHRON"))
        .build()?
        .try_deserialize()?;
    Ok(settings)
}
