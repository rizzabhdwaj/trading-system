use crate::OMS::order_management::{display_orderbook, fetch_order, get_balance, get_my_orders};
use crate::api_gateway::metrics_handler::get_metrics;
use crate::auth;
use crate::domain::market::{MarketData, SocketServer};
use crate::matching_engine::orderbook::OrderBook;
use crate::metrics::MetricsCollector;
use crate::middleware::auth_middleware::validator;

use std::sync::{Arc, Mutex};

use actix_cors::Cors;
use actix_web::{App, HttpServer, web};
use actix_web_httpauth::middleware::HttpAuthentication;

const PORT: u16 = 8080;

pub async fn api_gateway(
    socket_server: Arc<SocketServer>,
    market: Arc<Mutex<MarketData>>,
    orderbook: Arc<Mutex<OrderBook>>,
    pool: sqlx::PgPool,
    metrics: Arc<MetricsCollector>,
) -> std::io::Result<()> {
    println!("Trading engine running on http://127.0.0.1:{PORT}");
    HttpServer::new(move || {
        let auth = HttpAuthentication::bearer(validator);
        let cors = Cors::permissive();
        App::new()
            .wrap(cors)
            .app_data(web::Data::new(Some(pool.clone())))
            .app_data(web::Data::new(orderbook.clone()))
            .app_data(web::Data::new(market.clone()))
            .app_data(web::Data::new(socket_server.clone()))
            .app_data(web::Data::new(metrics.clone()))
            .service(get_metrics)
            .service(auth::login::login_user)
            .service(auth::register::register_user)
            .service(
                web::scope("")
                    .wrap(auth)
                    .service(fetch_order)
                    .service(display_orderbook)
                    .service(get_balance)
                    .service(get_my_orders),
            )
    })
    .bind(("0.0.0.0", PORT))?
    .run()
    .await
}
