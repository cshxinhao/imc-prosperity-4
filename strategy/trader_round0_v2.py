from datamodel import OrderDepth, UserId, TradingState, Order
from typing import List
import string


PRODUCT_LIMITs = {
    "EMERALDS": 80,
    "TOMATOES": 80,
}


class Trader:
    """
    Version 1

    Observations:
    * The demands of EMERALDS is limited, to post with narrower bid-ask spread cannot yield more trades / PNL.
    * Add more indicative signals for TOMATOES, e.g. price momentum, volume imbalance, etc.
    """

    def bid(self):
        return 15

    def _get_fair_price(self, product: str, state: TradingState):
        """Determine the fair price for a product based on the current state of the market."""

        if product == "EMERALDS":
            fair_price = 10000
        elif product == "TOMATOES":
            order_depth: OrderDepth = state.order_depths[product]
            ask_price_1, ask_volume_1 = list(order_depth.sell_orders.items())[0]
            bid_price_1, bid_volume_1 = list(order_depth.buy_orders.items())[0]
            ask_volume = sum([b for a, b in list(order_depth.sell_orders.items())])
            bid_volume = sum([b for a, b in list(order_depth.buy_orders.items())])

            # Start with mid price
            fair_price = (ask_price_1 + bid_price_1) / 2
            print(f"fair_price start with mid price: {fair_price}")

            # Last tick pct change

            # Volume imbalance
            volume_imbalance = ask_volume - bid_volume / (ask_volume + bid_volume)
            fair_price = fair_price + (-0.005812) * volume_imbalance
            print(f"fair_price after volume imbalance: {fair_price}")

        return int(fair_price)

    def _check_position_limit(self, product: str, state: TradingState):
        """Check if the position for a product exceeds the limit."""
        position = state.position.get(product, 0)
        assert abs(position) <= PRODUCT_LIMITs[product], (
            f"Position {position} for product {product} exceeds the limit {PRODUCT_LIMITs[product]}"
        )

    def _calc_max_order_size(
        self, product: str, state: TradingState, orders: list[Order]
    ):
        """Calculate the maximum order size for a product based on the current position and limit."""
        position = state.position.get(product, 0)

        # From current position (as of last trade day)
        max_buy_size = (
            PRODUCT_LIMITs[product] - position
        )  # E.g. If position = -10, max_buy_size = 90
        max_sell_size = (
            -PRODUCT_LIMITs[product] - position
        )  # E.g. If position = 10, max_sell_size = -90

        # From generated orders
        for order in orders:
            if order.product == product:
                # Buy order
                if order.quantity > 0:
                    max_buy_size = max_buy_size - order.quantity  # E.g 8-5=3

                # Sell order
                elif order.quantity < 0:
                    max_sell_size = max_sell_size - order.quantity  # E.g -8-(-5)=-3

        # Sanity check
        assert max_buy_size >= 0, "max_buy_size should be non-negative"
        assert max_sell_size <= 0, "max_sell_size should be non-positive"

        return max_buy_size, max_sell_size

    def post_orders(self, fair_price: float, product: str, state: TradingState):
        """
        Post liquidity to the market
        """
        order_depth: OrderDepth = state.order_depths[product]
        orders: List[Order] = []

        # Fix bid spread = 8 and ask spread = 8
        bid_spread = 8
        ask_spread = 8
        if len(order_depth.buy_orders) != 0:
            best_bid, best_bid_amount = list(order_depth.buy_orders.items())[0]
            if int(best_bid) < fair_price - bid_spread:
                print("SELL", str(best_bid_amount) + "x", best_bid)
                orders.append(Order(product, best_bid, -best_bid_amount))

        if len(order_depth.sell_orders) != 0:
            best_ask, best_ask_amount = list(order_depth.sell_orders.items())[0]
            if int(best_ask) > fair_price + ask_spread:
                print("BUY", str(-best_ask_amount) + "x", best_ask)
                orders.append(Order(product, best_ask, -best_ask_amount))

        return orders

    def run(self, state: TradingState):
        """Only method required. It takes all buy and sell orders for all
        symbols as an input, and outputs a list of orders to be sent."""

        print("traderData: " + state.traderData)
        print("Observations: " + str(state.observations))

        # Orders to be placed on exchange matching engine
        result = {}
        for product in state.order_depths:
            # Total orders for this product
            orders: List[Order] = []

            # Display current orders on market
            order_depth: OrderDepth = state.order_depths[product]
            print(
                "Buy Order depth : "
                + str(len(order_depth.buy_orders))
                + ", Sell order depth : "
                + str(len(order_depth.sell_orders))
            )

            # Get the fair price for the product
            fair_price = self._get_fair_price(product, state)
            print("Acceptable price : " + str(fair_price))

            # ---------------------------------------------------------------------------------------------------
            # Generate orders - take liquidity from the market
            if len(order_depth.sell_orders) != 0:
                best_ask, best_ask_amount = list(order_depth.sell_orders.items())[0]

                # Buy: take the liquidity of ask 1
                if int(best_ask) < fair_price:
                    buyable_size = -best_ask_amount
                    max_buy_size, _ = self._calc_max_order_size(product, state, orders)
                    buy_price = best_ask
                    buy_size = min(buyable_size, max_buy_size)
                    print("Active BUY", str(buy_size) + "x", buy_price)
                    orders.append(Order(product, buy_price, buy_size))

            if len(order_depth.buy_orders) != 0:
                best_bid, best_bid_amount = list(order_depth.buy_orders.items())[0]

                # Sell: take the liquidity of bid 1
                if int(best_bid) > fair_price:
                    sellable_size = -best_bid_amount
                    _, max_sell_size = self._calc_max_order_size(product, state, orders)
                    sell_price = best_bid
                    sell_size = -min(abs(sellable_size), abs(max_sell_size))
                    print("Active SELL", str(sell_size) + "x", sell_price)
                    orders.append(Order(product, sell_price, sell_size))

            # ---------------------------------------------------------------------------------------------------
            # Generate orders - post liquidity to the market
            max_buy_size, max_sell_size = self._calc_max_order_size(
                product, state, orders
            )
            # EMERALDS strategy
            if product == "EMERALDS":
                # Half
                bid_spread = 7
                ask_spread = 7
                # Post bid orders
                bid_size = int(max_buy_size / 2)
                bid_price = int(fair_price - bid_spread)
                orders.append(Order(product, bid_price, bid_size))
                print("Passive BUY", str(bid_size) + "x", bid_price)
                # Post ask orders
                ask_size = int(max_sell_size / 2)
                ask_price = int(fair_price + ask_spread)
                orders.append(Order(product, ask_price, ask_size))
                print("Passive SELL", str(ask_size) + "x", ask_price)

            else:
                bid_spread = 6
                ask_spread = 6

                # Post bid orders
                bid_size = max_buy_size
                bid_price = int(fair_price - bid_spread)
                orders.append(Order(product, bid_price, bid_size))
                print("Passive BUY", str(bid_size) + "x", bid_price)
                # Post ask orders
                ask_size = max_sell_size
                ask_price = int(fair_price + ask_spread)
                orders.append(Order(product, ask_price, ask_size))
                print("Passive SELL", str(ask_size) + "x", ask_price)

            result[product] = orders

        # String value holding Trader state data required.
        # It will be delivered as TradingState.traderData on next execution.
        traderData = "SAMPLE"

        # Sample conversion request. Check more details below.
        conversions = 1
        return result, conversions, traderData
