import sys
import os
import asyncio
import time
import ast
from hubble_exchange import HubbleClient, ConfirmationMode
from dotenv import dotenv_values
import tools
from price_feeds import PriceFeed

# import marketMaker
import config
from typing import TypedDict
from hyperliquid_async import HyperLiquid
from binance_async import Binance
from connectors.bybit import Bybit
from orderManager import OrderManager

env = {**dotenv_values(".env.shared"), **dotenv_values(".env.secret")}
os.environ["HUBBLE_RPC"] = env["HUBBLE_RPC"]
os.environ["HUBBLE_WS_RPC"] = env["HUBBLE_WS_RPC"]
os.environ["HUBBLE_ENV"] = env["HUBBLE_ENV"]
os.environ["PRIVATE_KEY"] = env[sys.argv[1] + "_PRIVATE_KEY"]
os.environ["HUBBLE_INDEXER_API_URL"] = env["HUBBLE_INDEXER_API_URL"]

# settings = ast.literal_eval(env[sys.argv[1]])
settings = getattr(config, sys.argv[1])


async def exit_maker(unhandled_exception_encountered: asyncio.Event):
    await unhandled_exception_encountered.wait()
    print("Restarting application due to a unhandled task exception.")
    # call close functions here on all clients
    # send tg notif.
    os._exit(1)


async def main(market):
    hubble_client = HubbleClient(os.environ["PRIVATE_KEY"])
    unhandled_exception_encountered = asyncio.Event()
    mid_price_streaming_event = asyncio.Event()
    hubble_price_streaming_event = asyncio.Event()
    hedge_client_uptime_event = asyncio.Event()
    hedge_client = None
    price_feed = PriceFeed(unhandled_exception_encountered)
    mid_price_condition = asyncio.Condition()

    # this task manages exiting application in case of unhandled exceptions. This can be restarted with use of pm2 configuration.

    asyncio.create_task(exit_maker(unhandled_exception_encountered))

    try:
        if settings["priceFeed"] == "binance-futures":
            print("Starting feed")
            await price_feed.start_binance_futures_feed(
                market,
                mid_price_streaming_event,
                mid_price_condition,
            )
        elif settings["priceFeed"] == "bybit":
            print("Starting bybit feed")
            await price_feed.start_bybit_feed(
                settings["hedge_client_symbol"],
                mid_price_streaming_event,
                mid_price_condition,
            )
        else:
            asyncio.create_task(
                price_feed.start_binance_spot_feed(
                    market, mid_price_streaming_event, mid_price_condition
                )
            )

        markets = await hubble_client.get_markets()
        market_name = settings["name"]
        asset_name = market_name.split("-")[0]
        hubble_market_id = tools.get_key(markets, market_name)
        if settings["hedge_mode"]:
            if settings["hedge"] == "hyperliquid":
                hedge_client = HyperLiquid(
                    asset_name,
                    unhandled_exception_encountered,
                    {
                        "desired_max_leverage": settings["leverage"],
                        "slippage": settings["slippage"],
                    },
                )
            elif settings["hedge"] == "binance":
                hedge_client = Binance(
                    asset_name + "USDT",
                    unhandled_exception_encountered,
                    {
                        "desired_max_leverage": settings["leverage"],
                        "slippage": settings["slippage"],
                    },
                )
            elif settings["hedge"] == "bybit":
                hedge_client = Bybit(
                    settings["hedge_client_symbol"],
                    unhandled_exception_encountered,
                    {
                        "desired_max_leverage": settings["leverage"],
                        "slippage": settings["slippage"],
                    },
                )
            await asyncio.create_task(
                hedge_client.start(
                    hedge_client_uptime_event,
                    settings["hedgeClient_orderbook_frequency"],
                    settings["hedgeClient_user_state_frequency"],
                )
            )

        asyncio.create_task(
            price_feed.start_hubble_feed(
                hubble_client,
                hubble_market_id,
                settings["hubble_orderbook_frequency"],
                hubble_price_streaming_event,
            )
        )
        order_manager = OrderManager(unhandled_exception_encountered)

        await order_manager.start(
            price_feed,
            hubble_market_id,
            settings,
            hubble_client,
            hedge_client,
            mid_price_streaming_event,
            hubble_price_streaming_event,
            mid_price_condition,
            hedge_client_uptime_event,
        )

        # except Exception as e:
        #     print("Error in orderUpdater", e)
        #     restart_needed.set()
        #     returnf

        # await monitor_task

    except asyncio.CancelledError:
        print("asyncio.CancelledError")


# Start and run until complete
loop = asyncio.get_event_loop()
task = loop.create_task(main(sys.argv[1]))

# Run until a certain condition or indefinitely
try:
    loop.run_until_complete(task)
except KeyboardInterrupt:
    pass
    # # Handle other shutdown signals here
    # print("CANCELLING ORDERS AND SHUTTING DOWN")
    # task = loop.create_task(marketMaker.cancelAllOrders(hubble_client, marketID))
    # # if settings["hedge_mode"]:
    # #     asyncio.run(hedge_client.exit())
    # loop.run_until_complete(task)
