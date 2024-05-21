from pybit.unified_trading import WebSocket
import asyncio
import json
import os
import time
import websockets
from binance import AsyncClient, BinanceSocketManager

from hubble_exchange import HubbleClient, OrderBookDepthUpdateResponse

import tools


class PriceFeed:

    def __init__(self, unhandled_exception_encountered: asyncio.Event):
        self.loop = asyncio.get_event_loop()
        self.unhandled_exception_encountered = unhandled_exception_encountered
        self.mid_price = 0
        self.mid_price_last_updated_at = 0
        self.hubble_prices = [float("inf"), 0]  # [best_ask, best_bid]
        self.hubble_market_id = None
        self.hubble_client = None
        self.bybit_client = None
        self.is_hubble_price_feed_stopped = True
        self.is_bybit_price_feed_stopped = True
        self.binance_spot_feed_stopped = True
        self.binance_market_id = None
        self.binance_futures_feed_stopped = True
        self.mid_price_streaming_event: asyncio.Event = None
        self.mid_price_condition: asyncio.Condition = None

    async def start_hubble_feed(
        self, client: HubbleClient, market, freq, hubble_price_streaming_event
    ):
        print(f"Starting Hubble price feed for {market}...")
        self.hubble_market_id = market
        self.hubble_client = client

        asyncio.create_task(
            self.subscribe_to_hubble_order_book(freq, hubble_price_streaming_event)
        )

    async def subscribe_to_hubble_order_book(
        self, hubble_orderbook_frequency, hubble_price_streaming_event
    ):
        max_retries = 5
        attempt_count = 0
        retry_delay = 2

        async def callback(ws, response: OrderBookDepthUpdateResponse):
            if self.is_hubble_price_feed_stopped:
                hubble_price_streaming_event.set()
                self.is_hubble_price_feed_stopped = False
                # @todo check how to reset these values.
                attempt_count = 0  # Reset attempt counter on successful connection
                retry_delay = 1  # Reset retry delay on successful connection

            if response.bids is not None:
                filtered_bids = list(
                    filter(lambda x: abs(float(x[1])) > 0, response.bids)
                )
                sorted_bids = sorted(
                    filtered_bids, key=lambda x: float(x[0]), reverse=True
                )
                if (
                    len(sorted_bids) > 0
                    and float(sorted_bids[0][0]) > self.hubble_prices[1]
                ):
                    self.hubble_prices[1] = float(sorted_bids[0][0])

            if response.asks is not None:
                filtered_asks = list(
                    filter(lambda x: abs(float(x[1])) > 0, response.asks)
                )
                sorted_asks = sorted(filtered_asks, key=lambda x: float(x[0]))
                if (
                    len(sorted_asks) > 0
                    and float(sorted_asks[0][0]) < self.hubble_prices[0]
                ):
                    self.hubble_prices[0] = float(response.asks[0][0])

        while True:
            try:
                await self.hubble_client.subscribe_to_order_book_depth_with_freq(
                    self.hubble_market_id,
                    callback,
                    hubble_orderbook_frequency,
                )
            except Exception as e:
                if attempt_count >= max_retries:
                    print("Maximum retry attempts reached. Exiting price feed.")
                    # @todo check how to bubble the exceptionunhandled_exception_encountered
                    self.unhandled_exception_encountered.set()
                    raise e
                print("Error in start_hubble_feed err - ", e)
                # restart hubble feed
                hubble_price_streaming_event.clear()
                self.is_hubble_price_feed_stopped = True
                attempt_count += 1
                await asyncio.sleep(retry_delay)  # wait for retry_delay
                retry_delay *= 2  # Exponential backoff

    async def start_binance_spot_feed(
        self,
        market,
        mid_price_streaming_event: asyncio.Event,
        mid_price_condition: asyncio.Condition,
    ):
        symbol = tools.get_symbol_from_name(market) + "USDT"
        client = await AsyncClient.create()
        bm = BinanceSocketManager(client)
        # start any sockets here, i.e a trade socket
        ts = bm.trade_socket(symbol)
        # then start receiving messages
        async with ts as tscm:
            retry_delay = 3  # Initial retry delay in seconds
            max_retries = 5  # Maximum number of retries
            attempt_count = 0  # Attempt counter
            while True:
                try:

                    res = await tscm.recv()
                    if self.binance_spot_feed_stopped:
                        mid_price_streaming_event.set()
                        self.binance_spot_feed_stopped = False
                    price = float(res["p"])
                    self.mid_price = price
                    # @todo check the data for timestamp
                    # self.mid_price_last_updated_at = data["T"] / 1000
                    mid_price_condition.notify_all()
                except Exception as e:
                    if attempt_count >= max_retries:
                        print(
                            "Maximum retry attempts reached. Exiting binance spot feed."
                        )
                        self.unhandled_exception_encountered.set()
                        break
                    mid_price_streaming_event.clear()
                    self.binance_spot_feed_stopped = True
                    print(f"Binance futures feed connection error: {e}")
                    attempt_count += 1
                    print(
                        f"Attempting to reconnect in {retry_delay} seconds... (Attempt {attempt_count}/{max_retries})"
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
        await client.close_connection()

    async def start_binance_futures_feed(
        self,
        market,
        mid_price_streaming_event: asyncio.Event,
        mid_price_condition: asyncio.Condition,
    ):
        symbol = tools.get_symbol_from_name(market) + "USDT"
        print(f"Starting Binance Futures price feed for {symbol}...")
        task = asyncio.create_task(
            self.subscribe_to_binance_futures_feed(
                symbol, mid_price_streaming_event, mid_price_condition
            )
        )
        print("Binance Futures price feed started.")
        return task
        # ws_url = f"wss://fstream.binance.com/ws/{symbol.lower()}@depth@100ms"

    async def subscribe_to_binance_futures_feed(
        self,
        symbol,
        mid_price_streaming_event: asyncio.Event,
        mid_price_condition: asyncio.Condition,
    ):
        print(f"subscribe_to_binance_futures_feed for {symbol}...")
        ws_url = f"wss://fstream.binance.com/ws/{symbol.lower()}@bookTicker"

        retry_delay = 3  # Initial retry delay in seconds
        max_retries = 5  # Maximum number of retries
        attempt_count = 0  # Attempt counter
        while True:
            try:
                async with websockets.connect(ws_url) as websocket:
                    print("Connected to the server.")
                    if self.binance_futures_feed_stopped:
                        mid_price_streaming_event.set()
                        self.binance_futures_feed_stopped = False
                    attempt_count = 0  # Reset attempt counter on successful connection
                    retry_delay = 3  # Reset retry delay on successful connection
                    while True:
                        message = await websocket.recv()
                        data = json.loads(message)

                        self.mid_price = round(
                            (float(data["b"]) + float(data["a"])) / 2, 5
                        )
                        # print(f"Mid price: {self.mid_price}")
                        self.mid_price_last_updated_at = data["T"] / 1000
                        async with mid_price_condition:
                            mid_price_condition.notify_all()

            except Exception as e:
                if attempt_count >= max_retries:
                    print("Maximum retry attempts reached. Exiting.")
                    self.unhandled_exception_encountered.set()
                    break
                mid_price_streaming_event.clear()
                self.binance_futures_feed_stopped = True
                print(f"Binance futures feed connection error: {e}")
                attempt_count += 1
                print(
                    f"Attempting to reconnect in {retry_delay} seconds... (Attempt {attempt_count}/{max_retries})"
                )
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff

    ######### ByBit Feed #########

    async def start_bybit_feed(
        self,
        symbol,
        mid_price_streaming_event: asyncio.Event,
        mid_price_condition: asyncio.Condition,
    ):
        print(f"subscribe_to_bybit_futures_feed for {symbol}...")
        if self.bybit_client is None:
            # initialize client
            self.bybit_client = WebSocket(
                testnet=False,
                channel_type="linear",
            )
        level = 1  # since we only need the mid price we can subscribe to depth level 1 i.e top bid and top ask
        self.mid_price_streaming_event = mid_price_streaming_event
        self.mid_price_condition = mid_price_condition
        task = asyncio.create_task(
            self.subscribe_to_bybit_order_book(
                symbol, mid_price_streaming_event, mid_price_condition, level
            )
        )
        return task

    async def async_update_mid_price(self, mid_price, last_updated):
        if self.is_bybit_price_feed_stopped:
            self.mid_price_streaming_event.set()
            self.is_bybit_price_feed_stopped = False
        self.mid_price = mid_price
        self.mid_price_last_updated_at = last_updated
        async with self.mid_price_condition:
            self.mid_price_condition.notify_all()

    def bybit_callback(self, response):
        try:
            mid_price = (
                float(response["data"]["a"][0][0]) + float(response["data"]["b"][0][0])
            ) / 2
            last_updated = response["ts"]
            self.loop.create_task(self.async_update_mid_price(mid_price, last_updated))
        except Exception as e:
            print(f"Error in bybit_callback: {e}")
            self.unhandled_exception_encountered.set()

    async def subscribe_to_bybit_order_book(
        self,
        symbol,
        mid_price_streaming_event: asyncio.Event,
        mid_price_condition: asyncio.Condition,
        level=1,
    ):
        retry_delay = 1  # Initial retry delay in seconds
        max_retries = 5  # Maximum number of retries
        attempt_count = 0  # Attempt counter

        # while True:
        try:
            # check if connection is up
            self.bybit_client.orderbook_stream(level, symbol, self.bybit_callback)
        except Exception as err:
            # if attempt_count >= max_retries:
            #     print("Maximum retry attempts reached while fetching bybit price feed.")
            #     self.unhandled_exception_encountered.set()
            #     # break
            mid_price_streaming_event.clear()
            self.is_bybit_price_feed_stopped = True
            print(f"Bybit futures feed connection error: {err}")
            self.unhandled_exception_encountered.set()

            # attempt_count += 1
            # print(
            #     f"Attempting to reconnect to bybit in {retry_delay} seconds... (Attempt {attempt_count}/{max_retries})"
            # )
            # await asyncio.sleep(retry_delay)
            # retry_delay *= 2  # Exponential backoff

    def get_mid_price(self):
        return self.mid_price

    def get_mid_price_last_update_time(self):
        return self.mid_price_last_updated_at

    def get_hubble_prices(self):
        return self.hubble_prices
