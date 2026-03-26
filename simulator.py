import argparse
import csv
import importlib
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from datamodel import Listing, Observation, Order, OrderDepth, Trade, TradingState


@dataclass(frozen=True)
class PriceSnapshot:
    timestamp: int
    product: str
    bids: List[Tuple[int, int]]
    asks: List[Tuple[int, int]]
    mid_price: Optional[float]


def _parse_int(value: str) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return None
    if "." in s:
        return int(float(s))
    return int(s)


def _parse_float(value: str) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return None
    return float(s)


def load_prices(path: str) -> Tuple[List[int], Dict[int, Dict[str, PriceSnapshot]]]:
    per_ts: Dict[int, Dict[str, PriceSnapshot]] = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            ts = _parse_int(row["timestamp"])
            product = row["product"]
            if ts is None or product is None:
                continue

            bids: List[Tuple[int, int]] = []
            asks: List[Tuple[int, int]] = []

            for level in (1, 2, 3):
                bp = _parse_int(row.get(f"bid_price_{level}", ""))
                bv = _parse_int(row.get(f"bid_volume_{level}", ""))
                if bp is not None and bv is not None and bv != 0:
                    bids.append((bp, bv))

                ap = _parse_int(row.get(f"ask_price_{level}", ""))
                av = _parse_int(row.get(f"ask_volume_{level}", ""))
                if ap is not None and av is not None and av != 0:
                    asks.append((ap, av))

            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])

            snap = PriceSnapshot(
                timestamp=ts,
                product=product,
                bids=bids,
                asks=asks,
                mid_price=_parse_float(row.get("mid_price", "")),
            )
            per_ts.setdefault(ts, {})[product] = snap

    timestamps = sorted(per_ts.keys())
    return timestamps, per_ts


def load_market_trades(path: str) -> List[Trade]:
    trades: List[Trade] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            ts = _parse_int(row.get("timestamp", ""))
            sym = row.get("symbol", "")
            price = _parse_int(row.get("price", ""))
            qty = _parse_int(row.get("quantity", ""))
            if ts is None or sym is None or price is None or qty is None:
                continue

            buyer = (row.get("buyer") or "").strip()
            seller = (row.get("seller") or "").strip()

            trades.append(
                Trade(
                    symbol=sym,
                    price=price,
                    quantity=qty,
                    buyer=buyer,
                    seller=seller,
                    timestamp=ts,
                )
            )

    trades.sort(key=lambda t: t.timestamp)
    return trades


def bucket_trades_by_tick(
    market_trades: List[Trade], tick_timestamps: List[int]
) -> Dict[int, List[Trade]]:
    by_tick: Dict[int, List[Trade]] = {ts: [] for ts in tick_timestamps}
    i = 0
    n = len(market_trades)

    prev_ts = None
    for ts in tick_timestamps:
        while i < n and market_trades[i].timestamp <= ts:
            if prev_ts is None or market_trades[i].timestamp > prev_ts:
                by_tick[ts].append(market_trades[i])
            i += 1
        prev_ts = ts

    return by_tick


def build_order_depth(snapshot: PriceSnapshot) -> OrderDepth:
    od = OrderDepth()
    for price, vol in snapshot.bids:
        od.buy_orders[int(price)] = int(vol)
    for price, vol in snapshot.asks:
        od.sell_orders[int(price)] = -int(vol)
    return od


def load_trader(trader_spec: str):
    if ":" not in trader_spec:
        raise ValueError(
            'Trader spec must look like "strategy.trader_round0_v0:Trader"'
        )
    module_name, cls_name = trader_spec.split(":", 1)
    mod = importlib.import_module(module_name)
    cls = getattr(mod, cls_name)
    return cls()


def normalize_trader_output(output):
    if isinstance(output, dict):
        return output, 0, ""
    if isinstance(output, tuple) and len(output) == 3:
        result, conversions, trader_data = output
        return result, conversions, trader_data
    raise TypeError("Trader.run must return dict or (dict, conversions, traderData)")


def validate_orders_shape(result: dict) -> None:
    if not isinstance(result, dict):
        raise TypeError("run() result must be a dict mapping symbol->List[Order]")

    for symbol, orders in result.items():
        if not isinstance(symbol, str):
            raise TypeError("Order dict keys must be product symbols (str)")
        if orders is None:
            continue
        if not isinstance(orders, list):
            raise TypeError(f"Orders for {symbol} must be a list")
        for o in orders:
            if (
                not hasattr(o, "symbol")
                or not hasattr(o, "price")
                or not hasattr(o, "quantity")
            ):
                raise TypeError(
                    f"Order for {symbol} must have symbol/price/quantity attributes"
                )
            if not isinstance(o.price, int):
                raise TypeError(
                    f"Order.price must be int (got {type(o.price)} for {symbol})"
                )
            if not isinstance(o.quantity, int):
                raise TypeError(
                    f"Order.quantity must be int (got {type(o.quantity)} for {symbol})"
                )
            if o.quantity == 0:
                raise ValueError(f"Zero-quantity order in {symbol} is invalid")


def enforce_position_limits(
    result: dict, position: Dict[str, int], limits: Dict[str, int]
) -> dict:
    filtered = {}
    for symbol, orders in result.items():
        if orders is None:
            filtered[symbol] = []
            continue
        limit = limits.get(symbol, limits.get("*", 0))
        if limit <= 0:
            filtered[symbol] = orders
            continue

        start_pos = int(position.get(symbol, 0))
        delta = sum(int(o.quantity) for o in orders)
        worst_case = start_pos + delta
        if abs(worst_case) > limit:
            filtered[symbol] = []
        else:
            filtered[symbol] = orders
    return filtered


def match_orders_against_book(
    symbol: str,
    orders: List[Order],
    order_depth: OrderDepth,
    timestamp: int,
    cash: float,
    position: Dict[str, int],
    self_id: str = "YOU",
    market_id: str = "BOT",
) -> Tuple[List[Trade], float]:
    trades: List[Trade] = []

    asks = dict(order_depth.sell_orders)
    bids = dict(order_depth.buy_orders)

    for order in orders:
        if order.symbol != symbol:
            raise ValueError(
                f"Order.symbol {order.symbol} does not match bucket symbol {symbol}"
            )

        if order.quantity > 0:
            remaining = int(order.quantity)
            for px in sorted(asks.keys()):
                if remaining <= 0:
                    break
                if px > order.price:
                    break
                avail = -int(asks[px])
                if avail <= 0:
                    continue
                fill = min(remaining, avail)
                remaining -= fill
                asks[px] = int(asks[px]) + fill
                if asks[px] == 0:
                    del asks[px]

                trades.append(
                    Trade(
                        symbol=symbol,
                        price=int(px),
                        quantity=int(fill),
                        buyer=self_id,
                        seller=market_id,
                        timestamp=int(timestamp),
                    )
                )
                cash -= float(fill * px)
                position[symbol] = int(position.get(symbol, 0)) + int(fill)

        elif order.quantity < 0:
            remaining = int(-order.quantity)
            for px in sorted(bids.keys(), reverse=True):
                if remaining <= 0:
                    break
                if px < order.price:
                    break
                avail = int(bids[px])
                if avail <= 0:
                    continue
                fill = min(remaining, avail)
                remaining -= fill
                bids[px] = int(bids[px]) - fill
                if bids[px] == 0:
                    del bids[px]

                trades.append(
                    Trade(
                        symbol=symbol,
                        price=int(px),
                        quantity=int(fill),
                        buyer=market_id,
                        seller=self_id,
                        timestamp=int(timestamp),
                    )
                )
                cash += float(fill * px)
                position[symbol] = int(position.get(symbol, 0)) - int(fill)

    return trades, cash


def run_backtest(
    trader_spec: str,
    prices_path: str,
    trades_path: Optional[str],
    limits: Dict[str, int],
    start_ts: Optional[int],
    end_ts: Optional[int],
    max_ticks: Optional[int],
) -> None:
    trader = load_trader(trader_spec)

    tick_timestamps, snapshots = load_prices(prices_path)

    if start_ts is not None:
        tick_timestamps = [t for t in tick_timestamps if t >= start_ts]
    if end_ts is not None:
        tick_timestamps = [t for t in tick_timestamps if t <= end_ts]
    if max_ticks is not None:
        tick_timestamps = tick_timestamps[: max(0, int(max_ticks))]

    products = sorted({p for ts in tick_timestamps for p in snapshots[ts].keys()})
    listings = {
        p: Listing(symbol=p, product=p, denomination="XIRECS") for p in products
    }
    observations = Observation(plainValueObservations={}, conversionObservations={})

    market_trades: List[Trade] = []
    if trades_path:
        market_trades = load_market_trades(trades_path)
    market_trades_by_tick = bucket_trades_by_tick(market_trades, tick_timestamps)

    position: Dict[str, int] = {p: 0 for p in products}
    cash: float = 0.0
    trader_data: str = ""
    own_trades_last: Dict[str, List[Trade]] = {p: [] for p in products}

    last_mid: Dict[str, Optional[float]] = {p: None for p in products}

    for ts in tick_timestamps:
        order_depths = {p: build_order_depth(snapshots[ts][p]) for p in products}
        for p in products:
            last_mid[p] = snapshots[ts][p].mid_price

        state = TradingState(
            traderData=trader_data,
            timestamp=int(ts),
            listings=listings,
            order_depths=order_depths,
            own_trades=own_trades_last,
            market_trades={p: market_trades_by_tick.get(ts, []) for p in products},
            position=dict(position),
            observations=observations,
        )

        try:
            output = trader.run(state)
            result, conversions, trader_data = normalize_trader_output(output)
            validate_orders_shape(result)
        except Exception as e:
            raise RuntimeError(f"Trader.run failed at timestamp={ts}") from e

        result = enforce_position_limits(result, position, limits)

        own_trades_now: Dict[str, List[Trade]] = {p: [] for p in products}
        for p in products:
            orders = result.get(p, []) or []
            fills, cash = match_orders_against_book(
                symbol=p,
                orders=orders,
                order_depth=order_depths[p],
                timestamp=ts,
                cash=cash,
                position=position,
            )
            own_trades_now[p] = fills

        own_trades_last = own_trades_now

    mtm = 0.0
    for p, pos in position.items():
        mid = last_mid.get(p)
        if mid is not None:
            mtm += float(pos) * float(mid)

    total_pnl = cash + mtm

    print("Backtest finished")
    print(f"Trader: {trader_spec}")
    print(f"Ticks: {len(tick_timestamps)}")
    print(f"Cash PnL: {cash:.2f}")
    print(f"MTM: {mtm:.2f}")
    print(f"Total PnL: {total_pnl:.2f}")
    print("Final positions:")
    for p in sorted(position.keys()):
        print(f"  {p}: {position[p]}")


def parse_limits(limit_specs: List[str]) -> Dict[str, int]:
    limits: Dict[str, int] = {"*": 20}
    for spec in limit_specs:
        if "=" not in spec:
            raise ValueError('Limits must be like "EMERALDS=20" or "*=20"')
        k, v = spec.split("=", 1)
        limits[k.strip()] = int(v.strip())
    return limits


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--trader",
        required=True,
        help='Trader class spec like "strategy.trader_round0_v0:Trader"',
    )
    parser.add_argument(
        "--prices", required=True, help="Path to prices_round_0_day_*.csv"
    )
    parser.add_argument(
        "--trades", default="", help="Optional path to trades_round_0_day_*.csv"
    )
    parser.add_argument(
        "--limit",
        action="append",
        default=[],
        help='Per-product limit like "EMERALDS=20" (repeatable)',
    )
    parser.add_argument("--start-ts", type=int, default=None)
    parser.add_argument("--end-ts", type=int, default=None)
    parser.add_argument("--max-ticks", type=int, default=None)

    args = parser.parse_args(list(argv) if argv is not None else None)
    limits = parse_limits(args.limit)

    run_backtest(
        trader_spec=args.trader,
        prices_path=args.prices,
        trades_path=(args.trades.strip() or None),
        limits=limits,
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        max_ticks=args.max_ticks,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
