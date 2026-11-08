use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Trade {
    pub buyer_id: i32,
    pub seller_id: i32,
    pub qty: Decimal,
    pub price: Decimal,
}

#[derive(Serialize, Deserialize)]
pub struct TradeList {
    pub trades: Vec<Trade>,
}
