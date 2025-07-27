#!/usr/bin/env python3
import os
import glob
import pandas as pd
import numpy as np
import time
from pathlib import Path
from pybit.unified_trading import HTTP
from requests.exceptions import HTTPError
import logging

# ---- Cấu hình logging ----
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ---- Thư mục dữ liệu (dynamic) ----
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
history_dir = BASE_DIR / "bybit_full_history"

# ---- Thông tin API ----
API_KEY    = "fbUayzuilSFH13a87H"
API_SECRET = "mwJRm13UYelLHJsL00iyHFX2AjX2VPX6dL4N"
RECV_WINDOW = 60000

# ---- Khởi tạo session mainnet ----
session = HTTP(
    api_key=API_KEY,
    api_secret=API_SECRET,
    recv_window=RECV_WINDOW
)

# ---- Bỏ qua lỗi get_server_time ----
try:
    session.get_server_time()
except Exception as e:
    logging.warning("Ignoring get_server_time error: %s", e)

# ---- Thông số chiến lược ----
MIN_LOOKBACK = 26
TP_RATIO     = 1.02  # Take-profit ratio
MARGIN       = 1.0   # USD margin per trade
LEVERAGE     = 10    # Leverage factor
RATE_LIMIT_DELAY = 0.2  # seconds between API calls

def get_qty(symbol: str, entry_price: float):
    """Tính quantity và precision theo lotSizeFilter"""
    try:
        resp = session.get_instruments_info(category="linear", symbol=symbol)
        filt = resp["result"]["list"][0]["lotSizeFilter"]
        step = float(filt["qtyStep"])
        min_qty = float(filt["minOrderQty"])
    except HTTPError as e:
        logging.error("Error fetching symbol info for %s: %s", symbol, e)
        return 0, 0
    raw_qty = (MARGIN * LEVERAGE) / entry_price
    qty = max(min_qty, np.floor(raw_qty / step) * step) if step > 0 else raw_qty
    precision = abs(int(np.log10(step))) if 0 < step < 1 else 0
    return qty, precision

def find_entry_signals(df: pd.DataFrame):
    """Example signal: close < lower envelope; returns list of (entry, sl, tp)."""
    lower = df['lower']
    close = df['close']
    signals = []
    if len(df) > MIN_LOOKBACK:
        if close.iloc[-1] < lower.iloc[-1]:
            entry = df['nwe'].iloc[-1]
            sl = df['low'].iloc[-1]
            tp = entry * TP_RATIO
            signals.append((entry, sl, tp))
    return signals

def place_order(symbol: str, entry: float, sl: float, tp: float, precision: int):
    """Place market order and log result"""
    try:
        qty = entry  # adjust if using get_qty-derived qty
        resp = session.place_order(
            symbol=symbol,
            side="Buy",
            order_type="Market",
            qty=qty,
            time_in_force="IOC",
            reduce_only=False
        )
        logging.info("Order placed for %s: qty=%s, sl=%s, tp=%s", symbol, entry, sl, tp)
    except HTTPError as e:
        logging.error("Failed to place order for %s: %s", symbol, e)

def main():
    files = sorted(glob.glob(str(history_dir / "*_1h.csv")))
    if not files:
        logging.error("No CSV files found in %s", history_dir)
        return

    for fp in files:
        df = pd.read_csv(fp)
        signals = find_entry_signals(df)
        if not signals:
            continue
        for entry, sl, tp in signals:
            symbol = Path(fp).stem.replace("_1h", "")
            qty, precision = get_qty(symbol, entry)
            if qty <= 0:
                continue
            place_order(symbol, entry, sl, tp, precision)
            time.sleep(RATE_LIMIT_DELAY)

if __name__ == "__main__":
    main()
