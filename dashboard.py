import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots


@st.cache_data
def load_df(file):
    if isinstance(file, str):
        path = file
        if str(path).lower().endswith((".parquet", ".pq")):
            return pd.read_parquet(path)
        return pd.read_csv(path)

    name = getattr(file, "name", "")
    if str(name).lower().endswith((".parquet", ".pq")):
        return pd.read_parquet(file)
    return pd.read_csv(file)


def ensure_types(df):
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    if "day" in df.columns:
        df["day"] = df["day"].astype(str)
    if "product" in df.columns:
        df["product"] = df["product"].astype(str)
    return df


def _has_data(view: pd.DataFrame, col: str) -> bool:
    return col in view.columns and view[col].notna().any()


def build_figure(view: pd.DataFrame, product: str, levels, theme: str, show_trades: bool, marker_scale: float):
    
    template = "plotly_dark" if theme == "Dark" else "plotly_white"

    fig = make_subplots(
        rows=5,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.36, 0.14, 0.14, 0.14, 0.22],
        subplot_titles=(
            f"{product} - Order Book Prices + Trades",
            "Volumes + Volume Imbalance",
            "Spreads (Level 1-3)",
            "Bid/Ask Amount Imbalance",
            "Cumulative P&L + Returns",
        ),
    )

    dash_map = {1: "solid", 2: "dash", 3: "dot"}
    for i in levels:
        bp = f"bid_price_{i}"
        ap = f"ask_price_{i}"
        if _has_data(view, bp):
            fig.add_trace(
                go.Scatter(
                    x=view["timestamp"],
                    y=view[bp],
                    mode="lines",
                    name=f"Bid {i}",
                    line=dict(color="#1f77b4", width=2 if i == 1 else 1, dash=dash_map.get(i, "solid")),
                ),
                row=1,
                col=1,
            )
        if _has_data(view, ap):
            fig.add_trace(
                go.Scatter(
                    x=view["timestamp"],
                    y=view[ap],
                    mode="lines",
                    name=f"Ask {i}",
                    line=dict(color="#ff7f0e", width=2 if i == 1 else 1, dash=dash_map.get(i, "solid")),
                ),
                row=1,
                col=1,
            )

    if _has_data(view, "mid_price"):
        fig.add_trace(
            go.Scatter(
                x=view["timestamp"],
                y=view["mid_price"],
                mode="lines",
                name="Mid Price",
                line=dict(color="black" if template == "plotly_dark" else "#e0e0e0", width=3),
            ),
            row=1,
            col=1,
        )

    if show_trades and _has_data(view, "price"):
        trades = view[view["price"].notna()].copy()
        if not trades.empty:
            td = (
                trades["trade_direction"].astype(str).str.strip().str.lower()
                if "trade_direction" in trades.columns
                else pd.Series(index=trades.index, data="")
            )
            side = np.where(td.str.startswith("b"), "BUY", np.where(td.str.startswith("s"), "SELL", ""))
            qty = (
                pd.to_numeric(trades["quantity"], errors="coerce")
                if "quantity" in trades.columns
                else pd.Series(index=trades.index, data=1)
            ).fillna(1)
            size = np.clip(qty.to_numpy() * marker_scale + 6, 6, 28)
            color = np.where(side == "BUY", "#2ca02c", np.where(side == "SELL", "#d62728", "#7f7f7f"))
            text = pd.Series(index=trades.index, data="")
            if "buyer" in trades.columns or "seller" in trades.columns or "quantity" in trades.columns or "trade_direction" in trades.columns:
                buyer = trades["buyer"] if "buyer" in trades.columns else ""
                seller = trades["seller"] if "seller" in trades.columns else ""
                text = (
                    "Buyer: "
                    + buyer.astype(str)
                    + "<br>Seller: "
                    + seller.astype(str)
                    + "<br>Qty: "
                    + qty.astype(str)
                    + "<br>Dir: "
                    + td.astype(str)
                )

            fig.add_trace(
                go.Scatter(
                    x=trades["timestamp"],
                    y=trades["price"],
                    mode="markers",
                    name="Trades",
                    marker=dict(size=size, color=color, line=dict(width=1, color="white")),
                    text=text,
                    hovertemplate="%{text}<br>Price: %{y}<extra></extra>" if text.notna().any() else "Price: %{y}<extra></extra>",
                ),
                row=1,
                col=1,
            )

    if _has_data(view, "bid_volume"):
        fig.add_trace(go.Scatter(x=view["timestamp"], y=view["bid_volume"], mode="lines", name="Bid Vol Total", line=dict(color="#1f77b4")), row=2, col=1)
    if _has_data(view, "ask_volume"):
        fig.add_trace(go.Scatter(x=view["timestamp"], y=view["ask_volume"], mode="lines", name="Ask Vol Total", line=dict(color="#ff7f0e")), row=2, col=1)
    if _has_data(view, "volume_imbalance"):
        fig.add_trace(go.Scatter(x=view["timestamp"], y=view["volume_imbalance"], mode="lines", name="Volume Imbalance", line=dict(color="purple", dash="dash")), row=2, col=1)

    for i in [1, 2, 3]:
        sp = f"spread_{i}"
        if _has_data(view, sp):
            fig.add_trace(go.Scatter(x=view["timestamp"], y=view[sp], mode="lines", name=f"Spread {i}"), row=3, col=1)

    if _has_data(view, "amount_imbalance"):
        fig.add_trace(go.Scatter(x=view["timestamp"], y=view["amount_imbalance"], mode="lines", name="Amount Imbalance", line=dict(color="teal")), row=4, col=1)

    if _has_data(view, "profit_and_loss"):
        fig.add_trace(go.Scatter(x=view["timestamp"], y=view["profit_and_loss"], mode="lines", name="Cumulative P&L", line=dict(color="green", width=3)), row=5, col=1)
    if _has_data(view, "returns_5t"):
        fig.add_trace(go.Scatter(x=view["timestamp"], y=view["returns_5t"], mode="lines", name="5-tick Returns", line=dict(color="orange", dash="dot")), row=5, col=1)

    fig.update_layout(
        template=template,
        hovermode="x unified",
        legend=dict(orientation="h"),
        margin=dict(l=40, r=20, t=70, b=40),
        height=1050,
    )

    fig.update_yaxes(title_text="price", row=1, col=1)
    fig.update_yaxes(title_text="volume", row=2, col=1)
    fig.update_yaxes(title_text="spread", row=3, col=1)
    fig.update_yaxes(title_text="imbalance", row=4, col=1)
    fig.update_yaxes(title_text="pnl / returns", row=5, col=1)
    fig.update_xaxes(title_text="timestamp", row=5, col=1)
    fig.update_xaxes(rangeslider=dict(visible=True), row=5, col=1)

    return fig


st.set_page_config(layout="wide", page_title="Orderbook Dashboard")
st.title("Orderbook Dashboard")

uploaded = st.sidebar.file_uploader("Data file", type=["csv", "parquet", "pq"])
if uploaded is None:
    st.info("Upload a CSV or Parquet file containing the required fields.")
    st.stop()

df = ensure_types(load_df(uploaded))

missing = [c for c in ["day", "timestamp", "product"] if c not in df.columns]
if missing:
    st.error("Missing columns: " + ", ".join(missing))
    st.stop()

products = sorted(pd.Series(df["product"].dropna().unique()).astype(str))
product = st.sidebar.selectbox("Product", options=products)

days = sorted(
    pd.Series(df[df["product"] == product]["day"].dropna().unique()).astype(str)
)
day = st.sidebar.selectbox("Day", options=days)

dff = df[(df["product"] == product) & (df["day"] == day)].copy()
if dff.empty:
    st.warning("No data for selection.")
    st.stop()

dff.sort_values("timestamp", inplace=True)

tmin = int(np.nanmin(dff["timestamp"].values))
tmax = int(np.nanmax(dff["timestamp"].values))

theme = st.sidebar.selectbox("Theme", options=["Dark", "Light"], index=0)
levels = st.sidebar.multiselect("Levels", options=[1, 2, 3], default=[1, 2, 3])
show_trades = st.sidebar.checkbox("Show trades", value=True)
marker_scale = st.sidebar.slider("Trade marker scale", min_value=0.1, max_value=5.0, value=2.0)

start, end = st.sidebar.slider(
    "Time range", min_value=int(tmin), max_value=int(tmax), value=(int(tmin), int(tmax))
)

view = dff[(dff["timestamp"] >= start) & (dff["timestamp"] <= end)]

fig = build_figure(
    view=view,
    product=product,
    levels=levels,
    theme=theme,
    show_trades=show_trades,
    marker_scale=marker_scale,
)

st.plotly_chart(fig, use_container_width=True)

st.caption("Use the sidebar to choose the product, day, and time range.")
