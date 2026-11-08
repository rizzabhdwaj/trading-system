CREATE TABLE balances (
    user_id INTEGER REFERENCES users(id),
 
    balance_btc NUMERIC NOT NULL DEFAULT 0 CHECK (balance_btc >= 0),
    balance_inr NUMERIC NOT NULL DEFAULT 0 CHECK (balance_inr >= 0),
 
    reserved_btc NUMERIC NOT NULL DEFAULT 0 CHECK (reserved_btc >= 0),
    reserved_inr NUMERIC NOT NULL DEFAULT 0 CHECK (reserved_inr >= 0)
);
