use std::net::SocketAddr;

use axum::{
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use chron_base::load_config;
use chron_db::ChronDb;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
};

mod api;

#[derive(Clone)]
pub struct AppState {
    db: ChronDb,
}

pub struct AppError(anyhow::Error);

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = load_config()?;
    let db = ChronDb::new(&config).await?;

    let state = AppState { db };

    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_origin(Any);

    // let trace = TraceLayer::new_for_http()
    //     .on_request(DefaultOnRequest::new().level(Level::INFO))
    //     .on_response(DefaultOnResponse::new().level(Level::INFO));

    let app = Router::new()
        .route("/v0/game-events", get(api::get_game_events))
        .route("/v0/events", get(api::get_events))
        // .route("/v2/entities", get(api::get_entities))
        .route("/v0/versions", get(api::get_versions))
        // todo: is the order here right?
        .layer(cors)
        .layer(CompressionLayer::new())
        // .layer(trace)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}
