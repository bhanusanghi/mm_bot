# import asyncio
# import time
# from decimal import *
# import json

# from hexbytes import HexBytes
# from hubble_exchange import HubbleClient, ConfirmationMode
# from hubble_exchange.constants import get_minimum_quantity, get_price_precision
# from hubble_exchange.utils import int_to_scaled_float
# from hyperliquid_async import HyperLiquid
# from binance_async import Binance
# import tools

# hubble_client: HubbleClient = None

# positions = None


# async def orderUpdater(
#     client: HubbleClient,
#     hedgeClient: HyperLiquid | Binance | None,
#     marketID,
#     settings,
#     active_order_direction,
# ):
#     global hubble_client
#     hubble_client = client
#     lastUpdatePrice = 0

#     expiry_duration = settings["order_expiry"]
#     max_leverage = settings["leverage"]
#     # max_position_size = settings["maxPositionSize"]
#     while True:
#         mid_price = tools.get_mid_price()
#         if mid_price == 0:
#             await asyncio.sleep(2)
#             continue
#         thisPosition = {}
#         global positions
#         for position in positions.positions:
#             if position["market"] == marketID:
#                 thisPosition = position

#         # @todo Currently reservedMargin is 0 from API
#         availableMargin = (
#             float(positions.margin)
#             - float(positions.reservedMargin if positions.reservedMargin else 0)
#         ) * float(settings["marginShare"])
#         # check if available margin is enough to place an order
#         availableMarginBid = availableMargin
#         availableMarginAsk = availableMargin
#         multiple = 0
#         defensiveSkewBid = 0
#         defensiveSkewAsk = 0
#         if len(thisPosition) > 0 and float(thisPosition["size"]) > 0:
#             # @todo this is wrong, instead do totalMargin - reservedMargin. Currently reservedMargin is 0 from API
#             availableMarginBid = availableMargin - (
#                 float(thisPosition["notionalPosition"]) / float(settings["leverage"])
#             )
#             multiple = (
#                 float(thisPosition["notionalPosition"]) / float(settings["leverage"])
#             ) / availableMargin
#             defensiveSkewBid = multiple * 10 * settings["defensiveSkew"] / 100
#         elif len(thisPosition) > 0 and float(thisPosition["size"]) < 0:
#             # @todo this is wrong, instead do totalMargin - reservedMargin. Currently reservedMargin is 0 from API
#             availableMarginAsk = availableMargin - (
#                 float(thisPosition["notionalPosition"]) / float(settings["leverage"])
#             )
#             multiple = (
#                 float(thisPosition["notionalPosition"]) / float(settings["leverage"])
#             ) / availableMargin
#             defensiveSkewAsk = multiple * 10 * settings["defensiveSkew"] / 100
#         print(
#             f"availableMarginBid: {availableMarginBid} availableMarginAsk: {availableMarginAsk} multiple: {multiple}"
#         ),

#         buyOrders = await generateBuyOrders(
#             marketID,
#             mid_price,
#             settings,
#             availableMarginBid,
#             defensiveSkewBid,
#             float(thisPosition.get("size", 0)),
#             expiry_duration,
#             hedgeClient,
#         )
#         sellOrders = await generateSellOrders(
#             marketID,
#             mid_price,
#             settings,
#             availableMarginAsk,
#             defensiveSkewAsk,
#             float(thisPosition.get("size", 0)),
#             expiry_duration,
#             hedgeClient,
#         )

#         signed_orders = []
#         signed_orders = buyOrders + sellOrders
#         # signed_orders = buyOrders

#         if len(signed_orders) > 0:
#             try:
#                 placed_orders = await client.place_signed_orders(
#                     signed_orders, tools.generic_callback
#                 )
#                 for idx, order in enumerate(placed_orders):
#                     price = int_to_scaled_float(signed_orders[idx].price, 6)
#                     quantity = int_to_scaled_float(
#                         signed_orders[idx].base_asset_quantity, 18
#                     )
#                     if order["success"] == True:
#                         print(f"{order['order_id']}: {quantity}@{price} : ✅")
#                         active_order_direction[order["order_id"]] = (
#                             1 if quantity > 0 else -1
#                         )
#                     else:
#                         print(
#                             f"{order['order_id']}: {quantity}@{price} : ❌; {order['error']}"
#                         )

#                 lastUpdatePrice = mid_price
#             except Exception as error:
#                 print("failed to place orders", error)

#         # aim to place the order exactly at the start of the second
#         now = time.time()
#         next_run = expiry_duration - (now % expiry_duration)
#         if next_run < 0:
#             next_run += expiry_duration
#         await asyncio.sleep(next_run)

#     # fundingRate = await client.get_funding_rate(marketID,time.time())
#     # fundingRate = fundingRate["fundingRate"]
#     # print("last funding rate:", fundingRate)

#     # nextFundingRate = await client.get_predicted_funding_rate(marketID)
#     # nextFundingRate = nextFundingRate["fundingRate"]
#     # print("next funding rate:",nextFundingRate)


# async def generateBuyOrders(
#     marketID,
#     midPrice,
#     settings,
#     availableMargin,
#     defensiveSkew,
#     currentSize,
#     expiry_duration,
#     hedge_client,
# ):
#     orders = []
#     amountOnOrder = 0
#     leverage = float(settings["leverage"])
#     for level in settings["order_levels"]:
#         order_level = settings["order_levels"][level]
#         spread = float(order_level["spread"]) / 100 + defensiveSkew
#         bidPrice = midPrice * (1 - spread)
#         roundedBidPrice = round(bidPrice, get_price_precision(marketID))
#         best_ask_on_hubble = tools.get_hubble_prices()[0]
#         if settings.get("avoid_crossing", False):
#             # shift the spread to avoid crossing
#             if roundedBidPrice >= best_ask_on_hubble:
#                 bidPrice = best_ask_on_hubble * (1 - spread)
#                 roundedBidPrice = round(bidPrice, get_price_precision(marketID))

#         amtToTrade = (availableMargin * leverage) / roundedBidPrice
#         qty = getQty(order_level, amtToTrade, marketID)
#         # check if this is hedgable
#         if hedge_client is not None:
#             if await hedge_client.can_open_position(qty) is False:
#                 print("not enough margin to hedge position")
#                 continue

#         reduceOnly = False
#         if qty == 0:
#             continue
#         # @todo enable this when we have reduce only orders enabled for maker book
#         # elif currentSize < 0 and qty * -1 >= currentSize + amountOnOrder:
#         #     reduceOnly = True
#         #     amountOnOrder = amountOnOrder + qty
#         availableMargin = availableMargin - ((qty * roundedBidPrice) / leverage)
#         # order = LimitOrder.new(marketID, qty, roundedBidPrice, reduceOnly, True)
#         order = hubble_client.prepare_signed_order(
#             marketID, qty, roundedBidPrice, reduceOnly, expiry_duration
#         )

#         orders.append(order)
#     return orders


# async def generateSellOrders(
#     marketID,
#     midPrice,
#     settings,
#     availableMargin,
#     defensiveSkew,
#     currentSize,
#     expiry_duration,
#     hedge_client,
# ):
#     orders = []
#     amountOnOrder = 0
#     leverage = float(settings["leverage"])
#     for level in settings["order_levels"]:
#         order_level = settings["order_levels"][level]
#         spread = float(order_level["spread"]) / 100 + defensiveSkew
#         askPrice = midPrice * (1 + spread)
#         roundedAskPrice = round(askPrice, get_price_precision(marketID))
#         if settings.get("avoid_crossing", False):
#             best_bid_on_hubble = tools.get_hubble_prices()[1]
#             if roundedAskPrice <= best_bid_on_hubble:
#                 askPrice = best_bid_on_hubble * (1 + spread)
#                 roundedAskPrice = round(askPrice, get_price_precision(marketID))

#         amtToTrade = (availableMargin * leverage) / roundedAskPrice
#         qty = getQty(order_level, amtToTrade, marketID) * -1
#         if hedge_client is not None:
#             if await hedge_client.can_open_position(qty) is False:
#                 print("not enough margin to hedge position")
#                 continue
#         # check if this is hedgable
#         reduceOnly = False
#         if qty == 0:
#             continue
#         # @todo enable this when we have reduce only orders enabled for maker book
#         # elif currentSize > 0 and qty * -1 <= currentSize + amountOnOrder:
#         #     reduceOnly = True
#         #     amountOnOrder = amountOnOrder + qty
#         availableMargin = availableMargin - ((qty * roundedAskPrice) / leverage)
#         # order = LimitOrder.new(marketID, qty, roundedAskPrice, reduceOnly, True)
#         order = hubble_client.prepare_signed_order(
#             marketID, qty, roundedAskPrice, reduceOnly, expiry_duration
#         )
#         orders.append(order)
#     return orders


# async def start_positions_feed(
#     client: HubbleClient, wait_duration=2, restart_needed=None
# ):
#     while True:
#         try:
#             global positions

#             positions = await client.get_margin_and_positions(tools.generic_callback)
#             now = time.time()
#             next_run = wait_duration - (now % wait_duration)
#             await asyncio.sleep(next_run)
#         except Exception as error:
#             print("failed to get positions", error)
#             restart_needed.set()


# # async def cancelAllOrders(client, marketID):
# #     global activeOrders
# #     if len(activeOrders) > 0:
# #         try:
# #             nonce = await client.get_nonce()
# #             cancelledOrders = await client.cancel_limit_orders(
# #                 activeOrders, True, tools.callback, {"nonce": nonce}
# #             )
# #             print("cancelledOrders", cancelledOrders)
# #             for orderResponse in cancelledOrders:
# #                 if orderResponse["success"] or orderResponse["error"] == "Filled":
# #                     for order in activeOrders:
# #                         if order.id == orderResponse["order_id"]:
# #                             activeOrders.remove(order)
# #             return
# #         except Exception as error:
# #             print("there was an exception in cancel_limit_orders", error)
# #             return
# # else:
# #     open_orders = await client.get_open_orders(marketID, tools.callback)
# #     tasks = []
# #     if len(open_orders) > 0:
# #         nonce = await client.get_nonce()  # refresh nonce
# #         for order in open_orders:
# #             tasks.append(
# #                 client.cancel_order_by_id(
# #                     HexBytes(order.OrderId), True, tools.placeOrdersCallback
# #                 )
# #             )
# #     await asyncio.gather(*tasks)
# #     return


# def getQty(level, amtToTrade, marketID):
#     if float(level["qty"]) < amtToTrade:
#         return float(level["qty"])
#     elif amtToTrade > get_minimum_quantity(marketID):
#         return float(
#             Decimal(str(amtToTrade)).quantize(
#                 Decimal(str(get_minimum_quantity(marketID))), rounding=ROUND_DOWN
#             )
#         )
#     else:
#         return 0
