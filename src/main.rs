mod MDS;
mod OMS;
mod api_gateway;
mod auth;
mod domain;
mod matching_engine;
mod metrics;
mod middleware;
mod orderbook_proto;
mod trading_engine;
use api_gateway::api;
use api_gateway::db;
use domain::market::{MarketData, SocketServer};
use dotenvy::dotenv;
use metrics::MetricsCollector;
use orderbook_proto::order_book_client::OrderBookClient;
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use tonic::transport::Channel;

async fn startup() -> Result<(PgPool, OrderBookClient<Channel>), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL").expect("database url not found");
    let pool = db::init_pool(&database_url).await?;
    let configured_address = std::env::var("ORDERBOOK_GRPC_URL")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let address = if configured_address.contains("://") {
        configured_address
    } else {
        format!("http://{configured_address}")
    };
    let client = OrderBookClient::connect(address).await?;

    Ok((pool, client))
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .init();

    let market = Arc::new(Mutex::new(MarketData::new()));
    let socket_server = Arc::new(SocketServer::new());
    let (pool, orderbook_client) = startup().await?;
    let metrics = Arc::new(MetricsCollector::new());

    let run_server = socket_server.clone();
    tokio::spawn(async move {
        if let Err(e) = run_server.run("127.0.0.1:7878").await {
            eprintln!("socket server failed: {e}");
        }
    });

    api::api_gateway(socket_server, market, orderbook_client, pool, metrics).await?;
    Ok(())
}
