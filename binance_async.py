import asyncio
import json
import os
import time
from aio_binance.futures.usdt.websocket.session import WebsocketSession
from dotenv import dotenv_values
import numpy as np
import pandas as pd
import websockets
from aio_binance.futures.usdt import Client, WsClient
from aio_binance.error_handler.error import BinanceException
from loguru import logger as loguru_logger

# from telegram_bot import get_telegram_bot
from utils import get_logger, timeit, with_slippage

logger = get_logger()


class Binance(object):
    def __init__(
        self,
        market,
        unhandled_exception_encountered: asyncio.Event,
        settings: dict = {"desired_max_leverage": 5, "slippage": 0.5},
    ):
        env = {**dotenv_values(".env.shared"), **dotenv_values(".env.secret")}
        os.environ["BINANCE_API_KEY"] = env["BINANCE_API_KEY"]
        os.environ["BINANCE_SECRET_KEY"] = env["BINANCE_SECRET_KEY"]
        self.symbol = market
        self.client = Client(
            key=os.environ.get("BINANCE_API_KEY"),
            secret=os.environ.get("BINANCE_SECRET_KEY"),
        )
        # self.ws_client = None
        self.ws_client = None
        self.position_size = 0
        self.refresh_state_task = None
        self.state = {}

        self.last_callback_time = None
        self.alert_threshold = 5  # Seconds

        # self.telegram = get_telegram_bot()
        self.desired_initial_leverage = settings["desired_max_leverage"]
        self.slippage = settings["slippage"]
        self.unhandled_exception_encountered = unhandled_exception_encountered
        self.hedge_client_uptime_event = None

    async def start(
        self, hedge_client_uptime_event: asyncio.Event, user_state_frequency=5
    ):
        # await self.client.change_private_leverage(symbol=self.symbol, leverage=5)
        logger.info("#### binance starting...")
        self.hedge_client_uptime_event = hedge_client_uptime_event
        await self.set_initial_leverage()
        # listen_key_response = await self.client.create_private_listen_key()
        # listen_key = listen_key_response['data']['listenKey']
        listen_key = None
        # logger.info(f'#### binance listen_key = {listen_key}')
        self.ws_client = WsClient(listen_key, ping_timeout=60)
        self.refresh_state_task = asyncio.create_task(
            self.sync_user_state(user_state_frequency)
        )

    async def exit(self):
        if self.refresh_state_task:
            self.refresh_state_task.cancel()
        # try:
        #     await self.client.delete_private_listen_key()
        # except BinanceException as e:
        #     pass

    async def set_initial_leverage(self):
        # check existing position size. Update only at the beginning
        if self.position_size == 0:
            try:

                await self.client.change_private_leverage(
                    symbol=self.symbol, leverage=self.desired_initial_leverage
                )
                logger.info(
                    f"#### binance set_initial_leverage = {self.desired_initial_leverage}"
                )
                # await self.telegram.send_notification(f"Binance initial leverage set to {self.initial_leverage}.")
            except BinanceException as e:
                logger.info(f"Error: binance set_initial_leverage = {e}")
                # await self.telegram.send_notification(f"Error: binance set_initial_leverage = {e}")
        else:
            logger.info(
                f"#### binance set_initial_leverage: position exists, not setting leverage = {self.desired_initial_leverage}"
            )
            # await self.telegram.send_notification(f"Binance position exists, not setting leverage = {self.initial_leverage}")
        # run this code independent of above
        try:
            response = await self.client.get_private_leverage_brackets(
                symbol=self.symbol
            )
            self.state.initial_leverage = response["data"]["brackets"][0][
                "initialLeverage"
            ]
        except Exception as e:
            logger.error(f"Error: binance fetch and set_initial_leverage = {e}")
            # await self.telegram.send_notification(f"Error: binance set_initial_leverage = {e}")

    async def sync_user_state(self, user_state_frequency) -> dict:
        while True:
            try:
                user_state = await self.client.get_private_account_info()
                margin = list(
                    filter(lambda x: x["asset"] == "USDT", user_state["data"]["assets"])
                )[0]

                # @todo check if this is the correct available balance
                available_margin = float(margin["walletBalance"])
                position_state = list(
                    filter(
                        lambda x: x["symbol"] == "AVAXUSDT",
                        user_state["data"]["positions"],
                    )
                )[0]
                # position_state = position_state['data'][0]
                notional = float(position_state["notional"])
                size = float(position_state["positionAmt"])
                entry_price = float(position_state["entryPrice"])
                # liquidation_px = position_state['liquidationPrice']
                # liquidation_price = float(liquidation_px) if liquidation_px else float(0)
                # unrealized_pnl = float(position_state['unRealizedProfit'])
                leverage = abs(notional / available_margin)
                self.position_size = size
                self.state = {
                    "entry_price": entry_price,
                    # 'liquidation_price': liquidation_price,
                    # 'unrealized_pnl': unrealized_pnl,
                    "size": size,
                    "leverage": leverage,
                    "available_margin": available_margin,
                }
                await asyncio.sleep(10)
            # except requests.exceptions.ConnectionError as e:
            #     logger.info(f'Error: connection error in sync_user_state = {e}')
            #     self.reset_connection()
            #     await asyncio.sleep(10)

            except Exception as e:
                logger.info(f"Error: error in binance sync_user_state = {e}")
                await asyncio.sleep(user_state_frequency)

    def get_state(self):
        return self.state
        # res = await self.client.get_private_account_info()
        # # res = await self.client.get_private_balance()
        # # res = await self.client.get_private_position_risk(self.symbol)
        # print(json.dumps(res, indent=4, sort_keys=True))

    async def can_open_position(self, size, price):
        price_with_slippage = with_slippage(price, self.slippage, size > 0)
        return (
            self.state["available_margin"]
            >= abs(size * price_with_slippage) / self.state["initial_leverage"]
        )

    # @todo check the conversion of price decimals as needed
    async def on_Order_Fill(self, size, price):
        retries = 4
        delay = 0.2
        filled_size = 0
        final_avg_fill_price = 0
        total_fee = 0
        if self.can_open_position(size, price):
            for i in range(retries):
                try:
                    print(f"✅✅Executing hedge trade on binance, attempt {i+1}✅✅")
                    # calculated only once to maintain slippage across partial fills
                    order_execution_response = await self.execute_market_order(
                        size - filled_size, price, False, self.slippage
                    )
                    final_avg_fill_price += order_execution_response["price"] * (
                        order_execution_response["filled_quantity"] / size
                    )
                    if order_execution_response["isCompletelyFilled"]:
                        print(
                            f"Hedge Trade executed successfully on attempt {i+1}, Opened a position of size {size}."
                        )
                        break
                    else:
                        filled_size += order_execution_response["filled_quantity"]
                        print(
                            f"Trade partially executed. Filled size = {filled_size}. Attempt {i+1}"
                        )
                except Exception as e:
                    print(f"Trade execution failed on attempt {i+1}: {e}")
                    # If this was the last attempt, re-raise the exception
                    if i == retries - 1:
                        raise e
                    # Wait before the next attempt
                    await asyncio.sleep(delay)
            # @todo return taker fee as well
            return final_avg_fill_price

        print(f"Hedge Trade cannot be executed. Insufficient margin. Attempt {i+1}")

    async def execute_market_order(
        self, quantity, price, reduce_only=False, slippage=0
    ):
        side = "BUY" if quantity > 0 else "SELL"
        reduce_only = "true" if reduce_only else "false"
        price_with_slippage = with_slippage(price, slippage, side == "BUY")

        response = await self.client.create_private_order(
            symbol=self.symbol,
            side=side,
            type_order="LIMIT",
            quantity=str(abs(quantity)),
            reduce_only=reduce_only,
            time_in_force="IOC",
            price=str(price_with_slippage),
            new_order_resp_type="RESULT",
        )

        filled_quantity = float(response["data"]["executedQty"]) * np.sign(quantity)
        self.position_size += filled_quantity

        filled_price = float(response["data"]["avgPrice"])

        return {
            "exchange": "binance",
            "isCompletelyFilled": filled_quantity == quantity,
            "filled_quantity": filled_quantity,
            "quantity": quantity,
            "remainingQuantity": quantity - filled_quantity,
            "price": filled_price,
        }
