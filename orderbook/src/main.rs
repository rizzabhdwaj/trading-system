mod domain;
mod matching_engine;

pub mod proto {
    tonic::include_proto!("orderbook");
}

use domain::order::Order;
use matching_engine::orderbook::OrderBook;
use proto::order_book_server::{OrderBook as OrderBookRpc, OrderBookServer};
use proto::{
    GetOrderBookRequest, GetOrderBookResponse, PlaceOrderRequest, PlaceOrderResponse,
    SetRestingOrderIdRequest, SetRestingOrderIdResponse, Trade as ProtoTrade,
};
use rust_decimal::Decimal;
use sqlx::{FromRow, PgPool};
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
use tonic::transport::Server;

#[derive(FromRow)]
struct PendingOrderRow {
    order_id: i32,
    user_id: i32,
    side: String,
    qty: Decimal,
    price: Decimal,
}

struct OrderBookRpcService {
    book: Arc<Mutex<OrderBook>>,
}

fn parse_decimal(value: &str, field: &str) -> Result<Decimal, Status> {
    value
        .parse::<Decimal>()
        .map_err(|_| Status::invalid_argument(format!("invalid {field}")))
}

#[tonic::async_trait]
impl OrderBookRpc for OrderBookRpcService {
    async fn place_order(
        &self,
        request: Request<PlaceOrderRequest>,
    ) -> Result<Response<PlaceOrderResponse>, Status> {
        let request = request.into_inner();
        if request.side != "BUY" && request.side != "SELL" {
            return Err(Status::invalid_argument("side must be BUY or SELL"));
        }

        let qty = parse_decimal(&request.qty, "qty")?;
        let price = parse_decimal(&request.price, "price")?;
        if qty <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(Status::invalid_argument("qty and price must be positive"));
        }

        let order = Order {
            order_id: None,
            user_id: request.user_id,
            side: request.side,
            qty,
            price,
            status: "pending".to_string(),
        };

        let result = self
            .book
            .lock()
            .map_err(|_| Status::internal("orderbook lock poisoned"))?
            .engine(order);

        let trades = result
            .trades
            .trades
            .into_iter()
            .map(|trade| ProtoTrade {
                buyer_id: trade.buyer_id,
                seller_id: trade.seller_id,
                qty: trade.qty.to_string(),
                price: trade.price.to_string(),
            })
            .collect();

        let resting_order = result.appends.map(|order| proto::Order {
            user_id: order.user_id,
            side: order.side,
            qty: order.qty.to_string(),
            price: order.price.to_string(),
        });

        Ok(Response::new(PlaceOrderResponse {
            command_id: request.command_id,
            trades,
            resting_order,
            fulfilled_order_ids: result.fulfilled_ids,
        }))
    }

    async fn get_order_book(
        &self,
        _request: Request<GetOrderBookRequest>,
    ) -> Result<Response<GetOrderBookResponse>, Status> {
        let json = self
            .book
            .lock()
            .map_err(|_| Status::internal("orderbook lock poisoned"))?
            .display_orderbook()
            .to_string();

        Ok(Response::new(GetOrderBookResponse {
            orderbook_json: json,
        }))
    }

    async fn set_resting_order_id(
        &self,
        request: Request<SetRestingOrderIdRequest>,
    ) -> Result<Response<SetRestingOrderIdResponse>, Status> {
        let request = request.into_inner();
        let price = parse_decimal(&request.price, "price")?;
        self.book
            .lock()
            .map_err(|_| Status::internal("orderbook lock poisoned"))?
            .set_last_resting_id(&request.side, price, request.order_id);

        Ok(Response::new(SetRestingOrderIdResponse {}))
    }
}

async fn load_orderbook(pool: &PgPool, book: &mut OrderBook) -> Result<(), sqlx::Error> {
    let rows = sqlx::query_as::<_, PendingOrderRow>(
        "SELECT order_id, user_id, side, qty, price
         FROM orders
         WHERE status = 'pending'
         ORDER BY order_id",
    )
    .fetch_all(pool)
    .await?;

    for row in rows {
        book.add_resting_order(Order {
            order_id: Some(row.order_id),
            user_id: row.user_id,
            side: row.side.to_uppercase(),
            qty: row.qty,
            price: row.price,
            status: "pending".to_string(),
        });
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let mut initial_book = OrderBook::new();
    load_orderbook(&pool, &mut initial_book).await?;

    let address = std::env::var("ORDERBOOK_GRPC_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;
    let service = OrderBookRpcService {
        book: Arc::new(Mutex::new(initial_book)),
    };

    println!("Orderbook gRPC service running on {address}");
    Server::builder()
        .add_service(OrderBookServer::new(service))
        .serve(address)
        .await?;

    Ok(())
}
