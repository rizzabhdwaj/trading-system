ALTER TABLE balances
    ADD CONSTRAINT balances_balance_btc_nonnegative CHECK (balance_btc >= 0),
    ADD CONSTRAINT balances_balance_inr_nonnegative CHECK (balance_inr >= 0),
    ADD CONSTRAINT balances_reserved_btc_nonnegative CHECK (reserved_btc >= 0),
    ADD CONSTRAINT balances_reserved_inr_nonnegative CHECK (reserved_inr >= 0);
