use crate::domain::order::Order;
use crate::domain::trades::Trade;
use crate::domain::trades::TradeList;
use rust_decimal::Decimal;
use std::cmp;

use std::collections::BTreeMap;
use std::collections::VecDeque;

// Result of a single pass through the matching engine.
pub struct EngineResult {
    pub trades: TradeList,
    pub appends: Option<Order>,
    pub fulfilled_ids: Vec<i32>,
}

pub struct OrderBook {
    bids: BTreeMap<Decimal, VecDeque<Order>>, // Limit BUY orders (people waiting to buy)
    asks: BTreeMap<Decimal, VecDeque<Order>>, // Limit SELL orders (people waiting to sell)
}

impl OrderBook {
    fn TradeToOrder(side: String, req: &Trade) -> Order {
        let mut id = req.seller_id;
        if side == "BUY" {
            id = req.buyer_id;
        }
        Order {
            order_id: None,
            user_id: id,
            side,
            qty: req.qty.into(),
            price: req.price.into(),
            status: "pending".to_string(),
        }
    }
    pub fn rollBack(&mut self, tradeData: &TradeList) {
        // in case of trade failure this function is called
        for trade in tradeData.trades.iter() {
            // the selling qty should be stored in orderbook bids BTreeMap as it is

            let bidOrder = OrderBook::TradeToOrder("BUY".to_string(), trade);
            if let Some(val) = self.bids.get_mut(&trade.price) {
                val.push_front(bidOrder);
            };

            let askOrder = OrderBook::TradeToOrder("SELL".to_string(), trade);
            if let Some(val) = self.asks.get_mut(&trade.price) {
                val.push_front(askOrder);
            };
        }
    }
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn add_resting_order(&mut self, order: Order) {
        if order.side == "BUY" {
            self.bids.entry(order.price).or_default().push_back(order);
        } else {
            self.asks.entry(order.price).or_default().push_back(order);
        }
    }

    pub fn display_orderbook(&self) -> serde_json::Value {
        serde_json::json!({
            "asks": self.asks,
            "bids": self.bids,
        })
    }

    pub fn set_last_resting_id(&mut self, side: &str, price: Decimal, order_id: i32) {
        match side {
            "BUY" => {
                if let Some(queue) = self.bids.get_mut(&price) {
                    if let Some(last) = queue.back_mut() {
                        last.order_id = Some(order_id);
                    }
                }
            }
            "SELL" => {
                if let Some(queue) = self.asks.get_mut(&price) {
                    if let Some(last) = queue.back_mut() {
                        last.order_id = Some(order_id);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn engine(&mut self, mut match_data: Order) -> EngineResult {
        let mut trades = TradeList { trades: Vec::new() };
        let mut appends: Option<Order> = None;
        let mut fulfilled_ids: Vec<i32> = Vec::new();

        if match_data.side == "BUY" {
            // Match against asks (lowest price first — BTreeMap iterates ascending)
            for (_price, queue) in self.asks.iter_mut() {
                if match_data.qty == Decimal::ZERO {
                    break;
                }
                for resting in queue.iter_mut() {
                    if match_data.qty == Decimal::ZERO {
                        break;
                    }
                    if resting.qty > Decimal::ZERO && match_data.price >= resting.price {
                        let qty = cmp::min(match_data.qty, resting.qty);
                        let price = resting.price;

                        resting.qty -= qty;
                        if resting.qty == Decimal::ZERO {
                            resting.status = "fulfilled".to_string();
                            if let Some(id) = resting.order_id {
                                fulfilled_ids.push(id);
                            }
                        }

                        match_data.qty -= qty;

                        trades.trades.push(Trade {
                            buyer_id: match_data.user_id,
                            seller_id: resting.user_id,
                            qty: Decimal::from(qty),
                            price,
                        });
                    }
                }
            }

            // Remove fully consumed price levels
            self.asks.retain(|_, queue| {
                queue.retain(|o| o.qty > Decimal::ZERO);
                !queue.is_empty()
            });

            if match_data.qty > Decimal::ZERO {
                self.add_resting_order(match_data.clone());
                appends = Some(match_data);
            }
        } else if match_data.side == "SELL" {
            // Match against bids (highest price first — use rev())
            for (_price, queue) in self.bids.iter_mut().rev() {
                if match_data.qty == Decimal::ZERO {
                    break;
                }
                for resting in queue.iter_mut() {
                    if match_data.qty == Decimal::ZERO {
                        break;
                    }
                    if resting.qty > Decimal::ZERO && match_data.price <= resting.price {
                        let qty = cmp::min(match_data.qty, resting.qty);
                        let price = resting.price;

                        resting.qty -= qty;
                        if resting.qty == Decimal::ZERO {
                            resting.status = "fulfilled".to_string();
                            if let Some(id) = resting.order_id {
                                fulfilled_ids.push(id);
                            }
                        }

                        match_data.qty -= qty;

                        trades.trades.push(Trade {
                            buyer_id: resting.user_id,
                            seller_id: match_data.user_id,
                            qty: Decimal::from(qty),
                            price,
                        });
                    }
                }
            }

            // Remove fully consumed price levels
            self.bids.retain(|_, queue| {
                queue.retain(|o| o.qty > Decimal::ZERO);
                !queue.is_empty()
            });

            if match_data.qty > Decimal::ZERO {
                self.add_resting_order(match_data.clone());
                appends = Some(match_data);
            }
        }

        EngineResult {
            trades,
            appends,
            fulfilled_ids,
        }
    }
}
