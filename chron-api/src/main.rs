use std::net::SocketAddr;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get, Router,
};
use chron_base::load_config;
use chron_db::{ChronDb};

mod api;

#[derive(Clone)]
pub struct AppState {
    db: ChronDb,
}

pub struct AppError;

impl From<anyhow::Error> for AppError {
    fn from(_: anyhow::Error) -> Self {
        AppError
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, "{}").into_response()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = load_config()?;
    let db = ChronDb::new(&config).await?;

    let state = AppState { db };

    let app = Router::new()
    .route("/v0/game-events", get(api::get_game_events))
    .route("/v0/events", get(api::get_events))
    .route("/v2/entities", get(api::get_entities))
    .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}
