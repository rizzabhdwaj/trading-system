use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct OrderRequest {
    pub user_id: i32,
    pub side: String,
    pub qty: i32,
    pub price: i32,
}

#[derive(Clone, Serialize)]
pub struct Order {
    pub order_id: Option<i32>,
    pub user_id: i32,
    pub side: String,
    pub qty: Decimal,
    pub price: Decimal,
    pub status: String,
}
