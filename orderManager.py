from binance_async import Binance
import asyncio
import time
import csv
from decimal import *
import json
from hexbytes import HexBytes
from hubble_exchange import HubbleClient, ConfirmationMode
from hubble_exchange.constants import get_minimum_quantity, get_price_precision
from hubble_exchange.models import TraderFeedUpdate
from hubble_exchange.utils import int_to_scaled_float
from hyperliquid_async import HyperLiquid
import tools
from enum import Enum
import cachetools
import websockets
import os
from typing import Union


class OrderManager(object):

    def __init__(self, unhandled_exception_encountered: asyncio.Event):
        self.unhandled_exception_encountered = unhandled_exception_encountered
        self.client = None
        self.market = None
        self.settings = None
        self.hedge_client = None
        self.order_data = None
        self.price_feed = None
        self.trader_data = None
        self.trader_data_last_updated_at = 0
        self.market_position = None
        self.mid_price = None
        self.mid_price_last_updated_at = 0
        self.position_polling_task = None
        self.create_orders_task = None
        # self.order_fill_cooldown_triggered = False
        self.is_order_fill_active = False
        self.is_trader_position_feed_active = False
        self.save_performance_task = None
        self.mid_price_condition = None
        self.performance_data = {
            "start_time": 0,
            "end_time": 0,
            "cumulative_trade_volume": 0,
            "trade_volume_in_period": 0,
            "orders_attempted": 0,
            "orders_placed": 0,
            "orders_filled": 0,
            "orders_failed": 0,
            "orders_hedged": 0,
            "hedge_spread_pnl": 0,
            # "order_cancellations_attempted": 0,
            # "orders_cancelled": 0,
            # "order_cancellations_failed": 0,
            # "taker_fee": 0, @todo add this
            # "maker_fee": 0, @todo add this
        }
        self.last_order_fill_time = 0  # is in seconds

    async def start(
        self,
        price_feed,
        market: str,
        settings: dict,
        client: HubbleClient,
        hedge_client: Union[HyperLiquid, Binance, None],
        mid_price_streaming_event: asyncio.Event,
        hubble_price_streaming_event: asyncio.Event,
        mid_price_condition: asyncio.Condition,
        hedge_client_uptime_event: Union[asyncio.Event, None] = None,
    ):
        self.performance_data["start_time"] = time.strftime("%d-%m-%Y %H:%M")
        self.client = client
        self.settings = settings
        self.hedge_client = hedge_client
        self.market = market
        self.order_data = cachetools.TTLCache(
            maxsize=128, ttl=self.settings["order_expiry"] + 2
        )
        self.price_feed = price_feed
        # monitor_task = asyncio.create_task(self.monitor_restart())
        t1 = asyncio.create_task(self.start_order_fill_feed())
        t2 = asyncio.create_task(
            self.start_trader_positions_feed(settings["hubble_position_poll_interval"])
        )

        self.save_performance_task = asyncio.create_task(self.save_performance())

        # this task can be on demand paused and started
        self.create_orders_task = asyncio.create_task(
            self.create_orders(
                mid_price_streaming_event,
                hubble_price_streaming_event,
                hedge_client_uptime_event,
            )
        )
        self.mid_price_condition = mid_price_condition
        print("returning tasks from order manager")
        future = await asyncio.gather(
            t1, t2, self.create_orders_task, self.save_performance_task
        )
        return future

    def is_stale_data(
        self,
        mid_price_update_time,
        price_expiry,
        position_last_update_time,
        position_expiry,
    ):
        return (
            time.time() - mid_price_update_time > price_expiry
            or time.time() - position_last_update_time > position_expiry
        )

    async def monitor_cancellation(self, signed_orders, mid_price, expiry_time):
        while time.time() < expiry_time:
            async with self.mid_price_condition:
                await self.mid_price_condition.wait()
                updated_mid_price = self.price_feed.get_mid_price()
                delta_mid_price_percentage = (
                    (updated_mid_price - mid_price) * 100
                ) / mid_price

                if (
                    abs(delta_mid_price_percentage)
                    >= self.settings["cancellation_threshold"]
                ):

                    cancelled_orders = await self.client.cancel_signed_orders(
                        signed_orders, False, tools.generic_callback
                    )
                    # self.performance_data["order_cancellations_attempted"] += len(
                    #     cancelled_orders
                    # )
                    # for order in enumerate(cancelled_orders):
                    #     if order["success"]:
                    #         self.performance_data["orders_cancelled"] += 1
                    #     else:
                    #         self.performance_data["order_cancellations_failed"] += 1
                    break

    async def wait_until_next_order(self):
        # aim to place the order exactly at the start of the second
        now = time.time()
        order_frequency = self.settings["order_frequency"]
        next_run = order_frequency - (now % order_frequency)
        if next_run < 0:
            next_run += order_frequency
        await asyncio.sleep(next_run)

    async def create_orders(
        self,
        mid_price_streaming_event,
        hubble_price_streaming_event,
        hedge_client_uptime_event: Union[asyncio.Event, None] = None,
    ):
        max_retries = 5
        attempt_count = 0
        retry_delay = 2
        while True:
            try:
                # check for all services to be active
                await mid_price_streaming_event.wait()
                await hubble_price_streaming_event.wait()
                if self.settings["hedge_mode"]:
                    await hedge_client_uptime_event.wait()

                # print("####### all clear #######")

                if (
                    self.is_order_fill_active is False
                    or self.is_trader_position_feed_active is False
                ):
                    print(
                        "Hubble OrderFill feed or Trader Hubble position feed not running. Retrying in 5 seconds..."
                    )
                    await asyncio.sleep(5)
                    continue
                # get mid price
                if (
                    time.time() - self.last_order_fill_time
                    < self.settings["order_fill_cooldown"]
                ):

                    print("order fill cooldown triggered, skipping order creation")
                    await self.wait_until_next_order()
                    continue
                self.mid_price = self.price_feed.get_mid_price()
                self.mid_price_last_updated_at = (
                    self.price_feed.get_mid_price_last_update_time()
                )

                if self.is_stale_data(
                    self.mid_price_last_updated_at,
                    self.settings["mid_price_expiry"],
                    self.trader_data_last_updated_at,
                    self.settings["position_data_expiry"],
                ):
                    # todo handle better or with config.
                    print("stale data, skipping order creation")
                    await self.wait_until_next_order()
                    continue

                (
                    free_margin_ask,
                    free_margin_bid,
                    defensive_skew_ask,
                    defensive_skew_bid,
                ) = self.get_free_margin_and_defensive_skew()

                buy_orders = await self.generate_buy_orders(
                    free_margin_bid,
                    defensive_skew_bid,
                )
                sell_orders = await self.generate_sell_orders(
                    free_margin_ask,
                    defensive_skew_ask,
                )
                signed_orders = []
                signed_orders = buy_orders + sell_orders

                if len(signed_orders) > 0:
                    order_time = time.strftime("%H:%M::%S")
                    print(f"placing {len(signed_orders)} orders, time - {order_time}")
                    await self.place_orders(signed_orders)

                attempt_count = 0
                retry_delay = 2
                await self.wait_until_next_order()
            except Exception as e:
                print(f"failed to create orders: {e}")
                if attempt_count >= max_retries:
                    print(
                        "Could not start Order Creation - Maximum retry attempts reached. Exiting."
                    )
                    self.unhandled_exception_encountered.set()
                    break
                self.unhandled_exception_encountered.set()
                attempt_count += 1
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
                # exit the application

    # async def set_order_fill_cooldown(self):
    #     self.order_fill_cooldown_triggered = True
    #     await asyncio.sleep(self.settings["order_fill_cooldown"])
    #     self.order_fill_cooldown_triggered = False

    async def place_orders(self, signed_orders):
        try:
            placed_orders = await self.client.place_signed_orders(
                signed_orders, tools.generic_callback
            )
            self.performance_data["orders_attempted"] += len(placed_orders)
            successful_orders = []
            for idx, order in enumerate(placed_orders):
                price = int_to_scaled_float(signed_orders[idx].price, 6)
                quantity = int_to_scaled_float(
                    signed_orders[idx].base_asset_quantity, 18
                )
                if order["success"]:
                    self.performance_data["orders_placed"] += 1
                    successful_orders.append(signed_orders[idx])
                    print(f"{order['order_id']}: {quantity}@{price} : ✅")
                    self.order_data[order["order_id"]] = signed_orders[idx]
                else:
                    self.performance_data["orders_failed"] += 1
                    print(
                        f"{order['order_id']}: {quantity}@{price} : ❌; {order['error']}"
                    )

            if (
                self.settings["cancel_orders_on_price_change"]
                and len(successful_orders) > 0
            ):
                asyncio.create_task(
                    self.monitor_cancellation(
                        successful_orders,
                        self.mid_price,
                        time.time() + self.settings["order_expiry"],
                    )
                )
        except Exception as error:
            print("failed to place orders", error)
            raise error

    # only supports cross margin mode
    def get_free_margin_and_defensive_skew(self):
        total_margin = float(self.trader_data.margin)
        total_notional = 0
        u_pnl = 0
        # @todo add this
        pending_funding = 0

        for position in self.trader_data.positions:
            u_pnl += float(position["unrealisedProfit"])
            total_notional += float(position["notionalPosition"])
            if position["market"] == self.market:
                self.market_position = position

        used_margin = total_notional / float(self.settings["leverage"])
        reserved_margin = float(self.trader_data.reservedMargin)

        # print(total_margin, used_margin, u_pnl, pending_funding, reserved_margin)

        # @todo reserved margin for current open orders need to be accounted
        free_margin = (
            total_margin - used_margin + u_pnl - pending_funding - reserved_margin
        )
        margin_allocated_for_market = free_margin * float(self.settings["marginShare"])
        defensive_skew = self.settings["defensiveSkew"]
        defensive_skew_bid = 0
        defensive_skew_ask = 0
        margin_bid = margin_allocated_for_market
        margin_ask = margin_allocated_for_market

        if self.market_position is not None:
            if float(self.market_position["size"]) > 0:
                margin_used_for_position = float(
                    float(self.market_position["notionalPosition"])
                ) / float(self.settings["leverage"])
                margin_ask = (free_margin + margin_used_for_position) * float(
                    self.settings["marginShare"]
                )
                multiple = (
                    float(self.market_position["notionalPosition"])
                    / float(self.settings["leverage"])
                ) / free_margin
                defensive_skew_bid = multiple * 10 * defensive_skew / 100

            if float(self.market_position["size"]) < 0:
                margin_used_for_position = float(
                    float(self.market_position["notionalPosition"])
                ) / float(self.settings["leverage"])
                # @todo check if for reduce only orders we need to add the margin_used_for_position
                margin_bid = (free_margin + margin_used_for_position) * float(
                    self.settings["marginShare"]
                )
                multiple = (
                    float(self.market_position["notionalPosition"])
                    / float(self.settings["leverage"])
                ) / free_margin
                defensive_skew_ask = multiple * 10 * defensive_skew / 100
        # print("margin_ask, margin_bid, defensive_skew_ask, defensive_skew_bid")
        # print(margin_ask, margin_bid, defensive_skew_ask, defensive_skew_bid)
        return (margin_ask, margin_bid, defensive_skew_ask, defensive_skew_bid)

    async def generate_buy_orders(
        self,
        available_margin,
        defensive_skew,
    ):
        # print("generating buy orders")
        orders = []
        leverage = float(self.settings["leverage"])
        for level in self.settings["order_levels"]:
            order_level = self.settings["order_levels"][level]
            spread = float(order_level["spread"]) / 100 + defensive_skew
            bid_price = self.mid_price * (1 - spread)
            rounded_bid_price = round(bid_price, get_price_precision(self.market))
            # following should not block the execution
            try:
                best_ask_on_hubble = self.price_feed.get_hubble_prices()[0]
                if self.settings.get("avoid_crossing", False):
                    # shift the spread to avoid crossing
                    if rounded_bid_price >= best_ask_on_hubble:
                        bid_price = best_ask_on_hubble * (1 - spread)
                        rounded_bid_price = round(
                            bid_price, get_price_precision(self.market)
                        )
            except Exception as e:
                print("failed to get best ask on hubble", e)

            max_position_size = (available_margin * leverage) / rounded_bid_price
            qty = self.get_qty(order_level, max_position_size)
            # check if this is hedgable
            if self.hedge_client is not None:
                if self.hedge_client.can_open_position(qty, rounded_bid_price) is False:
                    print(
                        "not enough margin to hedge position, skipping making on hubble"
                    )
                    continue

            reduce_only = False
            if qty == 0:
                continue
            # @todo enable this when we have reduce only orders enabled for maker book
            # elif currentSize < 0 and qty * -1 >= currentSize + amountOnOrder:
            #     reduce_only = True
            #     amountOnOrder = amountOnOrder + qty
            available_margin = available_margin - ((qty * rounded_bid_price) / leverage)

            order = self.client.prepare_signed_order(
                self.market,
                qty,
                rounded_bid_price,
                reduce_only,
                self.settings["order_expiry"],
            )

            orders.append(order)
        return orders

    async def generate_sell_orders(
        self,
        available_margin,
        defensive_skew,
    ):
        # print("generating sell orders")
        orders = []
        leverage = float(self.settings["leverage"])
        for level in self.settings["order_levels"]:
            order_level = self.settings["order_levels"][level]
            spread = float(order_level["spread"]) / 100 + defensive_skew
            ask_price = self.mid_price * (1 + spread)
            rounded_ask_price = round(ask_price, get_price_precision(self.market))
            if self.settings.get("avoid_crossing", False):
                best_bid_on_hubble = self.price_feed.get_hubble_prices()[1]
                if rounded_ask_price <= best_bid_on_hubble:
                    ask_price = best_bid_on_hubble * (1 + spread)
                    rounded_ask_price = round(
                        ask_price, get_price_precision(self.market)
                    )

            max_position_size = (available_margin * leverage) / rounded_ask_price
            qty = self.get_qty(order_level, max_position_size) * -1
            if self.hedge_client is not None:
                if self.hedge_client.can_open_position(qty, rounded_ask_price) is False:
                    print("not enough margin to hedge position")
                    continue

            reduce_only = False
            if qty == 0:
                continue
            # @todo enable this when we have reduce only orders enabled for maker book
            # elif currentSize > 0 and qty * -1 <= currentSize + amountOnOrder:
            #     reduce_only = True
            #     amountOnOrder = amountOnOrder + qty
            available_margin = available_margin - ((qty * rounded_ask_price) / leverage)
            # order = LimitOrder.new(self.market, qty, rounded_ask_price, reduce_only, True)
            order = self.client.prepare_signed_order(
                self.market,
                qty,
                rounded_ask_price,
                reduce_only,
                self.settings["order_expiry"],
            )
            orders.append(order)
        return orders

    def get_qty(self, level, max_position_size):
        if float(level["qty"]) < max_position_size:
            return float(level["qty"])
        elif max_position_size > get_minimum_quantity(self.market):
            return float(
                Decimal(str(max_position_size)).quantize(
                    Decimal(str(get_minimum_quantity(self.market))), rounding=ROUND_DOWN
                )
            )
        else:
            return 0

    # For trader Positions data feed
    async def start_trader_positions_feed(self, poll_interval):
        max_retries = 5
        attempt_count = 0
        retry_delay = 2
        while True:
            try:
                self.trader_data = await self.client.get_margin_and_positions(
                    tools.generic_callback
                )
                if self.is_trader_position_feed_active is False:
                    self.is_trader_position_feed_active = True
                self.trader_data_last_updated_at = time.time()
                attempt_count = 0
                retry_delay = 2
                await asyncio.sleep(poll_interval)
            except Exception as error:
                self.is_trader_position_feed_active = False
                print(
                    f"failed to get trader data try {attempt_count+1}/{max_retries}",
                    error,
                )
                if attempt_count >= max_retries:
                    print(
                        "Could not start TraderPositionsFeed - Maximum retry attempts reached. Exiting."
                    )
                    self.unhandled_exception_encountered.set()
                    break
                attempt_count += 1
                await asyncio.sleep(retry_delay)
                retry_delay *= 2

    # For order fill callbacks
    async def order_fill_callback(self, ws, response: TraderFeedUpdate):
        if response.EventName == "OrderMatched":
            print(
                f"{time.strftime('%H:%M:%S')} : ✅✅✅order {response.OrderId} has been filled. fillAmount: {response.Args['fillAmount']}✅✅✅"
            )
            order_data = self.order_data.get(response.OrderId, None)
            # self.performance_data["maker_fee"] += response.Args["openInterestNotional"] * trading fee percentage

            ## update performance data
            self.performance_data["orders_filled"] += 1
            self.performance_data["trade_volume_in_period"] += float(
                response.Args["fillAmount"]
            )  # fillAmount is abs value
            self.performance_data["cumulative_trade_volume"] += float(
                response.Args["fillAmount"]
            )  # fillAmount is abs value
            if order_data is None:
                print(
                    f"{time.strftime('%H:%M:%S')} : ❌❌❌order {response.OrderId} not found in placed_orders_data. Cant decide hedge direction❌❌❌"
                )
                return

            if self.settings["hedge_mode"]:
                try:
                    order_direction = 1 if order_data.base_asset_quantity > 0 else -1
                    # @todo add taker fee data here
                    avg_hedge_price = await self.hedge_client.on_Order_Fill(
                        float(response.Args["fillAmount"]) * order_direction * -1,
                        response.Args["price"],
                    )
                    self.performance_data["orders_hedged"] += 1
                    instant_pnl = 0
                    if order_direction == 1:
                        instant_pnl = response.Args["fillAmount"] * (
                            avg_hedge_price - response.Args["price"]
                        )
                    else:
                        instant_pnl = response.Args["fillAmount"] * (
                            response.Args["price"] - avg_hedge_price
                        )

                    self.performance_data["hedge_spread_pnl"] += instant_pnl
                except Exception as e:
                    print(f"failed to hedge order fill: {e}")
                    self.unhandled_exception_encountered.set()
                    # exit the application
            self.last_order_fill_time = time.time()
            # await self.set_order_fill_cooldown()

    async def start_order_fill_feed(self):
        max_retries = 5
        attempt_count = 0
        retry_delay = 2
        while True:
            try:
                self.is_order_fill_active = True
                print(
                    "Starting hubble trader feed to listen to order fill callbacks..."
                )
                await self.client.subscribe_to_trader_updates(
                    ConfirmationMode.head, self.order_fill_callback
                )
                retry_delay = 2
                attempt_count = 0

            except websockets.ConnectionClosed:
                self.is_order_fill_active = False
                print(
                    "@@@@ trader feed: Connection dropped; attempting to reconnect..."
                )
                if attempt_count >= max_retries:
                    print(
                        "Could not start OrderFillCallback - Maximum retry attempts reached. Exiting."
                    )
                    self.unhandled_exception_encountered.set()
                    break
                attempt_count += 1
                await asyncio.sleep(retry_delay)  # Wait before attempting to reconnect
                retry_delay *= 2  # Exponential backoff
            except Exception as e:
                print(f"failed at order fill feed: {e}")
                self.is_order_fill_active = False
                # close the bot if an unexpected error occurs
                self.unhandled_exception_encountered.set()
                break  # Exit the loop if an unexpected error occurs

    async def save_performance(self):
        print(f"saving performance data, data: {self.performance_data}")
        filename = f"performance/performance_data_market_{self.market}.csv"

        try:
            # Check if the file exists and is not empty
            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                # Open the file in read mode
                with open(filename, "r", newline="", encoding="utf-8") as csvfile:
                    # Read the last row
                    last_row_data = list(csv.reader(csvfile))[-1]
                    # Get the cumulative trade volume from the last row
                    self.performance_data["cumulative_trade_volume"] = float(
                        last_row_data[2]
                    )
        except Exception as e:
            print(f"failed to read last row from performance CSV: {e}")
            self.unhandled_exception_encountered.set()

        while True:
            try:
                with open(filename, "a", newline="", encoding="utf-8") as csvfile:
                    # Create a CSV writer
                    writer = csv.writer(csvfile)
                    if csvfile.tell() == 0:
                        # Write the header row
                        writer.writerow(self.performance_data.keys())

                    self.performance_data["end_time"] = time.strftime("%d-%m %H:%M:%S")
                    # Write the data row
                    writer.writerow(self.performance_data.values())
                    # await loop.run_in_executor(
                    #     None, writer.writerow, self.performance_data.values()
                    # )
                    # clear the performance data
                    self.performance_data = {
                        "start_time": time.strftime("%d-%m %H:%M:%S"),
                        "end_time": 0,
                        "cumulative_trade_volume": self.performance_data[
                            "cumulative_trade_volume"
                        ],
                        "trade_volume_in_period": 0,
                        "orders_attempted": 0,
                        "orders_placed": 0,
                        "orders_filled": 0,
                        "orders_failed": 0,
                        "orders_hedged": 0,
                        "hedge_spread_pnl": 0,
                        # "order_cancellations_attempted": 0,
                        # "orders_cancelled": 0,
                        # "order_cancellations_failed": 0,
                        # "taker_fee": 0,
                        # "maker_fee": 0,
                    }

                # Sleep for a certain amount of time
                await asyncio.sleep(
                    self.settings["performance_tracking_interval"]
                )  # sleep for 60 seconds
            except Exception as e:
                print(f"failed to save performance data: {e}")
                self.unhandled_exception_encountered.set()
