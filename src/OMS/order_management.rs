use crate::domain::market::{MarketData, SocketServer};
use crate::domain::order::{Order, OrderRequest};
use crate::matching_engine::orderbook::OrderBook;
use crate::metrics::MetricsCollector;
use crate::middleware::auth_middleware::Claims;
use crate::trading_engine::engine::settle_trades;
//
use actix_web::{HttpMessage, HttpRequest, HttpResponse, get, post, web};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::{Connection, PgPool, Postgres, Transaction};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

pub async fn reserve_sell_balance(
    tx: &mut Transaction<'_, Postgres>,
    user_id: i32,
    qty: i32,
    metrics: &MetricsCollector,
) -> Result<u64, sqlx::Error> {
    let start = Instant::now();

    let required_btc = Decimal::from(qty);

    let updated: Option<(Decimal, Decimal)> = sqlx::query_as(
        "UPDATE balances
         SET balance_btc = balance_btc - $1,
             reserved_btc = reserved_btc + $1
         WHERE user_id = $2
           AND $1 > 0
           AND balance_btc >= $1
         RETURNING balance_btc, reserved_btc",
    )
    .bind(required_btc)
    .bind(user_id)
    .fetch_optional(&mut **tx)
    .await?;

    if let Some((balance_btc, reserved_btc)) = &updated {
        if *balance_btc < Decimal::ZERO || *reserved_btc < Decimal::ZERO {
            return Err(sqlx::Error::Protocol(
                "negative BTC balance after reservation".to_string(),
            ));
        }
    }

    let elapsed = start.elapsed().as_micros() as u64;
    metrics.record_reserve_balance(elapsed);
    Ok(u64::from(updated.is_some()))
}

pub async fn reserve_buy_balance(
    tx: &mut Transaction<'_, Postgres>,
    user_id: i32,
    qty: i32,
    price: i32,
    metrics: &MetricsCollector,
) -> Result<u64, sqlx::Error> {
    let start = Instant::now();

    let required_inr = Decimal::from(qty) * Decimal::from(price);

    let updated: Option<(Decimal, Decimal)> = sqlx::query_as(
        "UPDATE balances
         SET balance_inr = balance_inr - $1,
             reserved_inr = reserved_inr + $1
         WHERE user_id = $2
           AND $1 > 0
           AND balance_inr >= $1
         RETURNING balance_inr, reserved_inr",
    )
    .bind(required_inr)
    .bind(user_id)
    .fetch_optional(&mut **tx)
    .await?;

    if let Some((balance_inr, reserved_inr)) = &updated {
        if *balance_inr < Decimal::ZERO || *reserved_inr < Decimal::ZERO {
            return Err(sqlx::Error::Protocol(
                "negative INR balance after reservation".to_string(),
            ));
        }
    }

    let elapsed = start.elapsed().as_micros() as u64;
    metrics.record_reserve_balance(elapsed);
    Ok(u64::from(updated.is_some()))
}

pub fn ConvertToOrder(req: &OrderRequest, user_id: i32) -> Order {
    Order {
        order_id: None,
        user_id,
        side: req.side.clone(),
        qty: req.qty.into(),
        price: req.price.into(),
        status: "pending".to_string(),
    }
}
fn lock_book(orderbook: &Arc<Mutex<OrderBook>>) -> Result<MutexGuard<'_, OrderBook>, HttpResponse> {
    orderbook.lock().map_err(|_| {
        HttpResponse::InternalServerError()
            .json(serde_json::json!({"fail_reason": "orderbook lock poisoned"}))
    })
}

fn lock_market(
    market_data: &Arc<Mutex<MarketData>>,
) -> Result<MutexGuard<'_, MarketData>, HttpResponse> {
    market_data.lock().map_err(|_| {
        HttpResponse::InternalServerError()
            .json(serde_json::json!({"fail_reason": "market data lock poisoned"}))
    })
}

#[post("/api/order")]
pub async fn fetch_order(
    req: HttpRequest,
    socket_server: web::Data<Arc<SocketServer>>,
    orderbook: web::Data<Arc<Mutex<OrderBook>>>,
    market_data: web::Data<Arc<Mutex<MarketData>>>,
    pool: web::Data<Option<PgPool>>,
    metrics: web::Data<Arc<MetricsCollector>>,
    req_body: web::Json<OrderRequest>,
) -> HttpResponse {
    let handler_start = Instant::now();

    let pool = match pool.get_ref() {
        Some(p) => p,
        None => {
            return HttpResponse::ServiceUnavailable()
                .json(serde_json::json!({"fail_reason": "database not connected"}));
        }
    };

    let exts = req.extensions();
    let Some(claims) = exts.get::<Claims>() else {
        return HttpResponse::Unauthorized()
            .json(serde_json::json!({"fail_reason": "missing auth claims"}));
    };

    if req_body.qty <= 0 {
        return HttpResponse::InternalServerError()
            .json(serde_json::json!(" Qty of asset must be valid "));
    }
    if req_body.price <= 0 {
        return HttpResponse::InternalServerError()
            .json(serde_json::json!("price of asset must be valid "));
    }

    let tx_start = Instant::now();

    let acquire_start = Instant::now();
    let mut conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({"fail_reason": e.to_string()}));
        }
    };
    metrics.record_pool_acquire(acquire_start.elapsed().as_micros() as u64);

    let begin_start = Instant::now();
    let mut tx = match conn.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({"fail_reason": e.to_string()}));
        }
    };
    // sqlx::query("select pg_advisory_xact_lock(1)")    .execute(&mut *tx).await;

    metrics.record_begin(begin_start.elapsed().as_micros() as u64);
    metrics.record_tx_begin(tx_start.elapsed().as_micros() as u64);

    if req_body.side == "SELL" {
        match reserve_sell_balance(&mut tx, claims.id, req_body.qty, &metrics).await {
            Ok(0) => {
                return HttpResponse::InternalServerError().json(serde_json::json!(format!(
                    " userId : {} Insufficient Balance :- \n Selling QTY : {}\n",
                    claims.id, req_body.qty
                )));
            }
            Err(e) => {
                return HttpResponse::InternalServerError()
                    .json(serde_json::json!({"fail_reason": e.to_string()}));
            }
            _ => {}
        }
    }

    if req_body.side == "BUY" {
        match reserve_buy_balance(&mut tx, claims.id, req_body.qty, req_body.price, &metrics).await
        {
            Ok(0) => {
                return HttpResponse::InternalServerError().json(serde_json::json!(format!(
                    " userId : {} Insufficient Balance :- \n Buying QTY : {}\n",
                    claims.id, req_body.qty
                )));
            }
            Err(e) => {
                return HttpResponse::InternalServerError()
                    .json(serde_json::json!({"fail_reason": e.to_string()}));
            }
            _ => {}
        }
    }

    let order = ConvertToOrder(&req_body, claims.id);

    let lock_start = Instant::now();
    let result = {
        let mut ob = match lock_book(&orderbook) {
            Ok(g) => g,
            Err(resp) => return resp,
        };
        metrics.record_orderbook_lock(lock_start.elapsed().as_micros() as u64);

        let match_start = Instant::now(); // orderboook matching starts
        let res = ob.engine(order.clone());
        metrics.record_matching(match_start.elapsed().as_micros() as u64); // orderbook matching ends 

        res
    };

    {
        let mut md = match lock_market(&market_data) {
            Ok(g) => g,
            Err(resp) => return resp,
        };
        for trade in result.trades.trades.iter() {
            md.on_trade(trade, socket_server.get_ref());
        }
    }

    let settle_start = Instant::now();

    match settle_trades(
        claims.id,
        &order,
        &result.trades,
        result.appends,
        result.fulfilled_ids,
        &mut tx,
    )
    .await
    {
        Ok((balances, new_order_id)) => {
            metrics.record_settle_trades(settle_start.elapsed().as_micros() as u64);

            let commit_start = Instant::now();
            if let Err(e) = tx.commit().await {
                return HttpResponse::InternalServerError()
                    .json(serde_json::json!({"fail_reason": e.to_string()}));
            }
            metrics.record_tx_commit(commit_start.elapsed().as_micros() as u64);

            if let Some(id) = new_order_id {
                let mut ob = match lock_book(&orderbook) {
                    Ok(g) => g,
                    Err(resp) => return resp,
                };
                ob.set_last_resting_id(&req_body.side, Decimal::from(req_body.price), id);
            }

            metrics.record_order_total(handler_start.elapsed().as_micros() as u64);
            HttpResponse::Ok().json(balances)
        }
        Err(e) => {
            // if the trade fails the orderbook data should be restored
            {
                let mut ob = match lock_book(&orderbook) {
                    Ok(g) => g,
                    Err(resp) => return resp,
                };

                //         ob.rollBack(&result.trades);
            }
            HttpResponse::InternalServerError()
                .json(serde_json::json!({"fail_reason": e.to_string()}))
        }
    }
}
#[get("/api/balance")]
pub async fn get_balance(req: HttpRequest, pool: web::Data<Option<PgPool>>) -> HttpResponse {
    let pool = match pool.get_ref() {
        Some(p) => p,
        None => {
            return HttpResponse::ServiceUnavailable()
                .json(serde_json::json!({"fail_reason": "database not connected"}));
        }
    };

    let exts = req.extensions();
    let Some(claims) = exts.get::<Claims>() else {
        return HttpResponse::Unauthorized()
            .json(serde_json::json!({"fail_reason": "missing auth claims"}));
    };

    let balance = sqlx::query_as::<_, crate::domain::common::Balances>(
        "SELECT user_id, balance_btc, balance_inr, reserved_btc, reserved_inr FROM balances WHERE user_id = $1",
    )
    .bind(claims.id)
    .fetch_optional(pool)
    .await;

    match balance {
        Ok(Some(b)) => HttpResponse::Ok().json(b),
        Ok(None) => HttpResponse::Ok().json(serde_json::json!({
            "balance_btc": 0, "balance_inr": 0,
            "reserved_btc": 0, "reserved_inr": 0,
        })),
        Err(e) => HttpResponse::InternalServerError()
            .json(serde_json::json!({"fail_reason": e.to_string()})),
    }
}

#[derive(sqlx::FromRow, Serialize)]
struct OrderRow {
    order_id: i32,
    side: String,
    qty: Decimal,
    price: Decimal,
    status: String,
    dateadded: chrono::NaiveDateTime,
}

#[get("/api/my-orders")]
pub async fn get_my_orders(req: HttpRequest, pool: web::Data<Option<PgPool>>) -> HttpResponse {
    let pool = match pool.get_ref() {
        Some(p) => p,
        None => {
            return HttpResponse::ServiceUnavailable()
                .json(serde_json::json!({"fail_reason": "database not connected"}));
        }
    };

    let exts = req.extensions();
    let Some(claims) = exts.get::<Claims>() else {
        return HttpResponse::Unauthorized()
            .json(serde_json::json!({"fail_reason": "missing auth claims"}));
    };

    let orders = sqlx::query_as::<_, OrderRow>(
        "SELECT order_id, side, qty, price, status, dateadded FROM orders WHERE user_id = $1 ORDER BY dateadded DESC",
    )
    .bind(claims.id)
    .fetch_all(pool)
    .await;

    match orders {
        Ok(rows) => HttpResponse::Ok().json(rows),
        Err(e) => HttpResponse::InternalServerError()
            .json(serde_json::json!({"fail_reason": e.to_string()})),
    }
}

#[get("/api/orderbook")]
pub async fn display_orderbook(orderbook: web::Data<Arc<Mutex<OrderBook>>>) -> HttpResponse {
    let ob = match lock_book(&orderbook) {
        Ok(g) => g,
        Err(resp) => return resp,
    };
    HttpResponse::Ok().json(serde_json::json!({"orderbook": ob.display_orderbook()}))
}
