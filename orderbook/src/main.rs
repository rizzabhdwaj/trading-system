mod domain;
mod matching_engine;

use actix_web::{App, HttpResponse, HttpServer, get, post, web};
use domain::order::{Order, OrderRequest};
use matching_engine::orderbook::OrderBook;
use std::sync::{Arc, Mutex};

#[get("/health")]
async fn health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({"status": "ok"}))
}

#[get("/api/orderbook")]
async fn orderbook(book: web::Data<Arc<Mutex<OrderBook>>>) -> HttpResponse {
    match book.lock() {
        Ok(book) => HttpResponse::Ok().json(book.display_orderbook()),
        Err(_) => HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "orderbook lock poisoned"})),
    }
}

#[post("/api/order")]
async fn submit_order(
    book: web::Data<Arc<Mutex<OrderBook>>>,
    request: web::Json<OrderRequest>,
) -> HttpResponse {
    if request.qty <= 0 || request.price <= 0 {
        return HttpResponse::BadRequest()
            .json(serde_json::json!({"error": "qty and price must be positive"}));
    }

    if request.side != "BUY" && request.side != "SELL" {
        return HttpResponse::BadRequest()
            .json(serde_json::json!({"error": "side must be BUY or SELL"}));
    }

    let order = Order {
        order_id: None,
        user_id: request.user_id,
        side: request.side.clone(),
        qty: request.qty.into(),
        price: request.price.into(),
        status: "pending".to_string(),
    };

    match book.lock() {
        Ok(mut book) => {
            let result = book.engine(order);
            HttpResponse::Ok().json(serde_json::json!({
                "trades": result.trades,
                "resting_order": result.appends,
                "fulfilled_order_ids": result.fulfilled_ids,
            }))
        }
        Err(_) => HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "orderbook lock poisoned"})),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let address = std::env::var("ORDERBOOK_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8081".to_string());
    let book = Arc::new(Mutex::new(OrderBook::new()));

    println!("Orderbook service running on http://{address}");
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(book.clone()))
            .service(health)
            .service(orderbook)
            .service(submit_order)
    })
    .bind(address)?
    .run()
    .await
}
