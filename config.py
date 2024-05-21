ETH = {
    "name": "ETH-Perp",
    "marginShare": 0.33,
    "leverage": 5,
    "order_frequency": 2,  # create new orders at this frequency
    "order_expiry": 2,
    "defensiveSkew": 0,
    "priceFeed": "binance-futures",
    "avoid_crossing": False,
    "order_levels": {
        "1": {"spread": 0.03, "qty": 0.08},
        "2": {"spread": 0.04, "qty": 0.09},
        "3": {"spread": 0.05, "qty": 0.1},
    },
    "hedge": "hyperliquid",
    "hedge_mode": True,  # enable hedge mode ?
    "slippage": 0.01,  # max slippage for hegde orders
    "mid_price_expiry": 1,  # expiry of mid price from price feed while generating new orders (Should be greater than binance feed frequency)
    "hedgeClient_orderbook_frequency": 2,  # binance/hyperliquid feed frequency
    "hedgeClient_user_state_frequency": 5,  # binance/hyperliquid feed frequency
    "order_fill_cooldown": 20,  # wait these many seconds before placing another order after one is filled.
    "hubble_position_poll_interval": 5,  # poll hubble for position data every x seconds
    "position_data_expiry": 10,  # expiry of position data for hubble positions (should be greater than hubble_position_poll_interval)
    "hubble_orderbook_frequency": "1s",  # update Hubble orderbook at this freq
    "performance_tracking_interval": 1800,  # track performance every x seconds as a new row in the csv
    "cancel_orders_on_price_change": False,  # cancel stale orders when price changes by cancellation threshold percentage
    "cancellation_threshold": 0.05,  # when new mid price deviates from the price orders were placed at by this percentage, cancel orders
}

AVAX = {
    "name": "AVAX-Perp",
    "marginShare": 0.33,
    "leverage": 5,
    "refreshTolerance": 0.03,  # unused
    "order_frequency": 2,  # create new orders at this frequency
    "order_expiry": 2,  # orders expire after this time
    "defensiveSkew": 0,  # Add a multiple of this to the spread when position skews in one side. Can be 0 with hedge mode
    "priceFeed": "binance-futures",
    "avoid_crossing": False,
    "order_levels": {
        "1": {"spread": 0.02, "qty": 3},
        "2": {"spread": 0.03, "qty": 4},
        "3": {"spread": 0.05, "qty": 5},
    },
    "hedge": "hyperliquid",  # binance/hyperliquid. binance is not implemented yet
    "hedge_mode": True,  # enable hedge mode ?
    "slippage": 0.01,  # max slippage for hegde orders
    "mid_price_expiry": 1,  # expiry of mid price from price feed while generating new orders (Should be greater than binance feed frequency)
    "hedgeClient_orderbook_frequency": 2,  # binance/hyperliquid feed frequency
    "hedgeClient_user_state_frequency": 5,  # binance/hyperliquid feed frequency
    "order_fill_cooldown": 20,  # wait these many seconds before placing another order after one is filled.
    "hubble_position_poll_interval": 5,  # poll hubble for position data every x seconds
    "position_data_expiry": 10,  # expiry of position data for hubble positions (should be greater than hubble_position_poll_interval)
    "hubble_orderbook_frequency": "1s",  # update Hubble orderbook at this freq
    "performance_tracking_interval": 1800,  # track performance every x seconds as a new row in the csv
    "cancel_orders_on_price_change": False,  # cancel stale orders when price changes by cancellation threshold percentage
    "cancellation_threshold": 0.05,  # when new mid price deviates from the price orders were placed at by this percentage, cancel orders
}

SOL = {
    "name": "SOL-Perp",
    "marginShare": 0.33,
    "leverage": 5,
    "order_frequency": 2,  # create new orders at this frequency
    "order_expiry": 2,
    "defensiveSkew": 0,
    "priceFeed": "binance-futures",
    "avoid_crossing": False,
    "order_levels": {
        "1": {"spread": 0.03, "qty": 1.0},
        "2": {"spread": 0.04, "qty": 1.2},
        "3": {"spread": 0.05, "qty": 1.2},
    },
    "hedge": "hyperliquid",
    "hedge_mode": True,  # enable hedge mode ?
    "slippage": 0.01,  # max slippage for hegde orders
    "mid_price_expiry": 1,  # expiry of mid price from price feed while generating new orders (Should be greater than binance feed frequency)
    "hedgeClient_orderbook_frequency": 2,  # binance/hyperliquid feed frequency
    "hedgeClient_user_state_frequency": 5,  # binance/hyperliquid feed frequency
    "order_fill_cooldown": 20,  # wait these many seconds before placing another order after one is filled.
    "hubble_position_poll_interval": 5,  # poll hubble for position data every x seconds
    "position_data_expiry": 10,  # expiry of position data for hubble positions (should be greater than hubble_position_poll_interval)
    "hubble_orderbook_frequency": "1s",  # update Hubble orderbook at this freq
    "performance_tracking_interval": 1800,  # track performance every x seconds as a new row in the csv
    "cancel_orders_on_price_change": False,  # cancel stale orders when price changes by cancellation threshold percentage
    "cancellation_threshold": 0.05,  # when new mid price deviates from the price orders were placed at by this percentage, cancel orders
}

COQ = {
    "name": "COQ-Perp",
    "hedge_client_symbol": "10000COQUSDT",
    "hubble_market_id": "3",
    "marginShare": 0.25,
    "leverage": 5,
    "order_frequency": 2,  # create new orders at this frequency
    "order_expiry": 2,
    "defensiveSkew": 0,
    "priceFeed": "bybit",
    "avoid_crossing": False,
    "order_levels": {
        "1": {"spread": 0.1, "qty": 1000},
        "2": {"spread": 0.12, "qty": 2000},
        "3": {"spread": 0.15, "qty": 3000},
    },
    "hedge": "bybit",
    "hedge_mode": True,  # enable hedge mode ?
    "slippage": 0.05,  # max slippage for hegde orders
    "mid_price_expiry": 1,  # expiry of mid price from price feed while generating new orders (Should be greater than binance feed frequency)
    "hedgeClient_orderbook_frequency": 2,  # binance/hyperliquid feed frequency
    "hedgeClient_user_state_frequency": 5,  # binance/hyperliquid feed frequency
    "order_fill_cooldown": 20,  # wait these many seconds before placing another order after one is filled.
    "hubble_position_poll_interval": 5,  # poll hubble for position data every x seconds
    "position_data_expiry": 10,  # expiry of position data for hubble positions (should be greater than hubble_position_poll_interval)
    "hubble_orderbook_frequency": "1s",  # update Hubble orderbook at this freq
    "performance_tracking_interval": 1800,  # track performance every x seconds as a new row in the csv
    "cancel_orders_on_price_change": False,  # cancel stale orders when price changes by cancellation threshold percentage
    "cancellation_threshold": 0.05,  # when new mid price deviates from the price orders were placed at by this percentage, cancel orders
}
