from pybit.unified_trading import WebSocket
from pybit.unified_trading import HTTP
import asyncio
import time
import pandas as pd
import os
from dotenv import dotenv_values
import aiohttp
from aiohttp import ClientResponse
import hmac
import hashlib
import json
from typing import Any
import numpy as np
from utils import get_logger, timeit, with_slippage
import requests
import uuid


class Bybit:
    def __init__(
        self,
        market,
        unhandled_exception_encountered: asyncio.Event,
        settings: dict = {"desired_max_leverage": 5, "slippage": 0.5},
    ) -> None:
        """
        Initializes the Bybit class with necessary API connections and account information.
        """
        env = {**dotenv_values(".env.shared"), **dotenv_values(".env.secret")}
        os.environ["BYBIT_KEY"] = env["BYBIT_KEY"]
        os.environ["BYBIT_SECRET"] = env["BYBIT_SECRET"]
        os.environ["BYBIT_TEST_KEY"] = env["BYBIT_TEST_KEY"]
        os.environ["BYBIT_TEST_SECRET"] = env["BYBIT_TEST_SECRET"]

        # variables
        self.client = None
        self.private_client = None
        self.orderbook_feed_task = None
        self.user_state_updater = None
        self.mid_price = 0
        self.best_bid = None
        self.best_ask = None
        self.bids = pd.DataFrame()
        self.asks = pd.DataFrame()
        self.market = market
        self.unhandled_exception_encountered = unhandled_exception_encountered
        self.price_feed_last_updated = None
        self.is_trader_feed_down = True
        self.is_price_feed_down = True
        self.desired_max_leverage = settings["desired_max_leverage"]
        self.slippage = settings["slippage"]
        self.state = {}
        self.position_size = 0
        self.hedge_client_uptime_event = None
        self.price_feed_uptime_event = None
        self.session = None
        self.active_orders = []
        self.placed_orders = {}
        self.filled_orders = []
        self.filled_order_data = {}

    # async def stream_orderbook(self, orderbook_frequency):

    # handle breaks in connection
    def initialize_client(self, is_private_required=False):
        if is_private_required and self.private_client is None:
            if (
                os.environ["BYBIT_TEST_KEY"] is not None
                and os.environ["BYBIT_TEST_SECRET"] is not None
            ):
                self.private_client = WebSocket(
                    testnet=True,
                    channel_type="private",
                    api_key=os.environ["BYBIT_TEST_KEY"],
                    api_secret=os.environ["BYBIT_TEST_SECRET"],
                    trace_logging=True,
                )
            else:
                raise ValueError("Bybit API keys not found")

        if self.client is None:
            # initialize a public client
            self.client = WebSocket(
                testnet=False,
                channel_type="linear",
            )

    async def start(
        self,
        hedge_client_uptime_event: asyncio.Event,
        price_feed_uptime_event: asyncio.Event,
        orderbook_frequency=5,
        user_state_frequency=5,
    ):
        print("Starting Bybit...")
        # create a client
        self.initialize_client()
        await self.set_initial_leverage()
        self.hedge_client_uptime_event = hedge_client_uptime_event
        self.price_feed_uptime_event = price_feed_uptime_event
        self.user_state_updater = asyncio.create_task(
            self.update_user_state(user_state_frequency)
        )
        self.orderbook_feed_task = asyncio.create_task(self.stream_orderbook())
        await asyncio.sleep(5)
        await self.on_Order_Fill(1000, self.mid_price, hubble_order_id=uuid.uuid4().hex)

    async def update_user_state(self, user_state_frequency):
        while True:
            try:
                # @todo add error handling
                endpoint = "/v5/position/list"
                query = f"category=linear&symbol={self.market}"
                response = await self.get(endpoint, query)
                user_position = response["result"]["list"][0]
                # verify symbol
                if user_position["symbol"] != self.market:
                    raise ValueError(
                        f"Symbol mismatch. Expected {self.market}, got {user_position['symbol']}"
                    )
                wallet_endpoint = "/v5/account/wallet-balance"
                wallet_query = "accountType=UNIFIED"
                wallet_response = await self.get(wallet_endpoint, wallet_query)
                # print("wallet_response = ", wallet_response)
                # print("user_position = ", wallet_response)
                user_wallet_data = wallet_response["result"]["list"][0]
                # , side, , positionValue,
                if self.is_trader_feed_down:
                    self.is_trader_feed_down = False
                    print("setting hedge_client_uptime_event")
                    self.hedge_client_uptime_event.set()
                self.state = {
                    "entry_price": user_position["avgPrice"],
                    "liquidation_price": user_position["liqPrice"],
                    "unrealized_pnl": user_position["unrealisedPnl"],
                    "size": user_position["size"],
                    "side": user_position["side"],
                    "leverage": user_position["leverage"],
                    "available_margin": user_wallet_data[
                        "totalAvailableBalance"
                    ],  # "totalAvailableBalance": "3.00326056",
                }
            except Exception as e:
                print(f"Error updating user state: {e}")
                self.unhandled_exception_encountered.set()
                self.is_trader_feed_down = True
                self.hedge_client_uptime_event.clear()
            await asyncio.sleep(user_state_frequency)

    def orderbook_stream_update_callback(self, response):
        print("Bybit orderbook update received")
        # bids are in the form 'b': ['0.04235', '1180']], 'a': [['0.04603', '1000020']]
        self.best_bid = response["data"]["b"][0][0]
        self.best_ask = response["data"]["a"][0][0]
        self.mid_price = (float(self.best_ask) + float(self.best_bid)) / 2
        # Process bid and ask dataframes
        self.bids = (
            pd.DataFrame(response["data"]["b"], columns=["price", "size"])
            .astype(float)
            .sort_values(by="price", ascending=False)
        )
        self.asks = (
            pd.DataFrame(response["data"]["a"], columns=["price", "size"])
            .astype(float)
            .sort_values(by="price")
        )

        self.price_feed_last_updated = response["ts"]

    async def stream_orderbook(self, level=50):
        # @todo add check if connection is down
        if self.client is None:
            # initialize client
            self.initialize_client()
        self.client.orderbook_stream(
            level, self.market, self.orderbook_stream_update_callback
        )

    # async def check_last_data_time():
    #     while True:
    #         if time.time() - self.lastUpdated > 0:
    #             print("Data delta = ", time.time() - self.lastUpdated)
    #         await asyncio.sleep(1)

    ######### Order Execution #########

    def can_open_position(self, size, price):
        # @todo add free margin check
        # price = self.get_fill_price(size)
        # price = with_slippage(price, self.slippage, size > 0)
        # return (
        #     self.state["available_margin"]
        #     >= abs(size * price) / self.desired_max_leverage
        # )
        return True

    async def set_initial_leverage(self):
        if self.position_size is 0:
            try:
                print("Setting initial leverage on Bybit...")
                leverage = self.desired_max_leverage
                endpoint = "v5/position/set-leverage"
                body = {
                    "category": "linear",
                    "symbol": self.market,
                    "buyLeverage": leverage,
                    "sellLeverage": leverage,
                }
                print(json.dumps(body))
                body_str = json.dumps(body)
                # {
                #     "retCode": 0,
                #     "retMsg": "OK",
                #     "result": {},
                #     "retExtInfo": {},
                #     "time": 1672281607343,
                # }
                response = await self.post(endpoint, body)
                print("set_initial_leverage response = ", response)
                if response["retMsg"] == "OK":
                    print(f"Successfully set leverage to {leverage} on Bybit.")
            except Exception as e:
                print(f"Error setting leverage on bybit: {e}")
        else:
            print(
                f"Skipping setting leverage for {self.market} on Bybit as position already exists."
            )

    async def on_Order_Fill(self, size, price, hubble_order_id=None):
        retries = 4
        delay = 1
        total_fee = 0
        final_avg_fill_price = 0
        if self.can_open_position(size, price):
            for i in range(retries):
                try:
                    print(f"✅✅Executing hedge trade attempt on bybit, {i+1}✅✅")
                    print(f"Size = {size}, Price = {price}")
                    order_id = await self.execute_trade(
                        size, False, price, self.slippage, hubble_order_id
                    )
                    if order_id:
                        # @todo how to send back the final fill price ??
                        # @todo wait for a few seconds and check the status

                        # total_fee += trade_fee
                        # final_avg_fill_price = avg_fill_price
                        break
                except Exception as e:
                    print(f"Trade execution failed on attempt {i+1}: {e}")
                    # If this was the last attempt, re-raise the exception
                    if i == retries:
                        raise e
                    # Wait before the next attempt
                    await asyncio.sleep(delay)
            # @todo return taker fee as well
            return final_avg_fill_price

        print(f"Hedge Trade cannot be executed. Insufficient margin. Attempt {i+1}")

    @timeit
    async def execute_trade(
        self, quantity, reduce_only=False, price=None, slippage=0, hubble_order_id=None
    ):
        if not price and not reduce_only:
            raise ValueError("bybit: Price must be set for non-reduce only")
        # Determine the trade type (buy or sell)
        side = np.sign(quantity)
        if side == 1:
            is_buy = True
        else:
            is_buy = False

        if slippage:
            price = with_slippage(price, slippage, is_buy)

        payload = {
            "category": "linear",
            "symbol": self.market,
            "orderType": "Limit",
            "timeInForce": "IOC",
            "side": "Buy" if is_buy else "Sell",
            "qty": abs(quantity),
            "price": price,
            "orderLinkId": hubble_order_id if hubble_order_id else uuid.uuid4().hex,
        }
        # payload_str = f'{{"category":"{payload["category"]}","symbol":"{payload["symbol"]}","orderType":"{payload["orderType"]}","timeInForce":"{payload["timeInForce"]}","side":"{payload["side"]}","qty":"{payload["qty"]}","price":"{payload["price"]}","orderLinkId":"{payload["orderLinkId"]}"}}'

        print("Bybit sending order now")

        endpoint = "/v5/order/create"

        try:
            response = await self.post(endpoint, payload)
            # response = await self.post(endpoint, json.dumps(payload))
            print("Bybit order response = ", response)
            if response.retMsg != "OK":
                raise Exception(
                    f"Error: placing order on Bybit. Response -> {response}"
                )
            order_id = response["result"]["orderId"]
            self.placed_orders[order_id] = {
                "side": "Buy" if is_buy else "Sell",
                "qty": abs(quantity),
                "price": price,
                "filled_qty": 0,
            }
            self.active_orders.append(order_id)
            return order_id
            # return {
            #     "exchange": "bybit",
            #     "isCompletelyFilled": filled_quantity == quantity,
            #     "filled_quantity": filled_quantity,
            #     "quantity": quantity,
            #     "remainingQuantity": quantity - filled_quantity,
            #     "price": avg_fill_price,
            #     # @todo add trade fee here
            #     # "trade_fee":
            # }

        except requests.exceptions.ConnectionError as e:
            print(f"ConnectionError: placing order on Bybit = {e}")
            raise e
        except Exception as e:
            print(f"Error: placing order on Bybit {e}")
            error = e
            raise e

    ######## HTTP Call handlers ########

    # Mainnet API Endpoints:
    #     https://api.bybit.com
    #     https://api.bytick.com
    # Testnet API Endpoints:
    #     https://api-testnet.bybit.com

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers={"Connection": "keep-alive"})
        return self.session

    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()

    # param payload is a string
    def generate_signature(self, timestamp, recv_window, payload, api_key, secret_key):
        param_str = timestamp + api_key + recv_window + payload
        hash = hmac.new(
            bytes(secret_key, "utf-8"), param_str.encode("utf-8"), hashlib.sha256
        )
        signature = hash.hexdigest()
        return signature

    # param payload is a json string of POST req body
    async def post(self, endpoint: str, payload=None) -> Any:
        if payload is None:
            payload = "{}"  # Empty JSON object

        timestamp = str(int(time.time() * 10**3))
        recv_window = "5000"  # 5 seconds validity for bybit to recv the request

        api_key = os.environ["BYBIT_KEY"]
        secret_key = os.environ["BYBIT_SECRET"]
        if api_key is None or secret_key is None:
            raise ValueError("Bybit API keys not found")

        payload_str = json.dumps(payload)
        signature = self.generate_signature(
            timestamp, recv_window, payload_str, api_key, secret_key
        )
        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }
        url = "https://api.bybit.com/" + endpoint

        session = await self.get_session()  # Ensure the session is ready

        print("Bybit sending POST request now", payload)
        response = await session.post(url, headers=headers, json=payload)
        await self._handle_exception(response)

        try:
            return await response.json()
        except ValueError:
            return {"error": f"Could not parse JSON: {await response.text()}"}

    async def get(self, endpoint: str, query: str) -> Any:
        # if payload is None:
        #     payload = "{}"  # Empty JSON object

        timestamp = str(int(time.time() * 10**3))
        recv_window = "5000"  # 5 seconds validity for bybit to recv the request

        api_key = os.environ["BYBIT_KEY"]
        secret_key = os.environ["BYBIT_SECRET"]
        if api_key is None or secret_key is None:
            raise ValueError("Bybit API keys not found")

        signature = self.generate_signature(
            timestamp, recv_window, query, api_key, secret_key
        )
        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            # "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }

        url = "https://api.bybit.com" + endpoint
        if query is not "":
            url = url + "?" + query
        session = await self.get_session()  # Ensure the session is ready

        response = await session.get(url, headers=headers)
        await self._handle_exception(response)

        try:
            return await response.json()
        except ValueError:
            return {"error": f"Could not parse JSON: {await response.text()}"}

    # @todo handle error codes better here.
    async def _handle_exception(self, response: ClientResponse):
        status_code = response.status
        if status_code < 400:
            return
        if 400 <= status_code < 500:
            try:
                response.json()
                err = json.loads(response.text)
            except json.JSONDecodeError:
                pass
                # raise ClientError(
                #     status_code, None, response.text, None, response.headers
                # )
            error_data = None
            if "data" in err:
                error_data = err["data"]
            # raise ClientError(
            #     status_code, err["code"], err["msg"], response.headers, error_data
            # )
        # raise ServerError(status_code, response.text)
