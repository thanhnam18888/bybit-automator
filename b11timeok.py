#!/usr/bin/env python3
import os
import glob
import math
import pandas as pd
import numpy as np
import time
from pathlib import Path
from pybit.unified_trading import HTTP
from pybit._exceptions import FailedRequestError
from requests.exceptions import HTTPError
import logging

# ---- Cấu hình logging ----
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')s %(levelname)s %(message)s')

# ---- Thư mục dữ liệu (dynamic) ----
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
history_dir = BASE_DIR / "bybit_full_history"

# ---- Thông số API mainnet ----
API_KEY      = "fbUayzuilSFH13a87H"
API_SECRET   = "mwJRm13UYelLHJsL00iyHFX2AjX2VPX6dL4N"
RECV_WINDOW  = 60000

# ---- Khởi tạo session mainnet ----
session = HTTP(
    api_key=API_KEY,
    api_secret=API_SECRET,
    recv_window=RECV_WINDOW
)

# Bỏ qua lỗi get_server_time
try:
    session.get_server_time()
except Exception as e:
    logging.warning("Ignoring get_server_time error: %s", e)

# ---- Tham số chiến lược ----
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
    except (HTTPError, FailedRequestError) as e:
        logging.error("Error fetching symbol info for %s: %s", symbol, e)
        return 0, 0
    raw_qty = (MARGIN * LEVERAGE) / entry_price
    if step > 0:
        qty = max(min_qty, math.floor(raw_qty / step) * step)
        precision = abs(int(math.log10(step))) if 0 < step < 1 else 0
    else:
        qty = raw_qty
        precision = 0
    return qty, precision

def find_signals(df: pd.DataFrame):
    """Signal khi giá cắt xuống biên dưới và không có lần nào trong look-back trước đó"""
    df = df.copy()
    df["close_prev"] = df["close"].shift(1)
    df["lower_prev"] = df["lower"].shift(1)
    cond_cross = (df["close"] < df["lower"]) & (df["close_prev"] >= df["lower_prev"])
    signals = []
    for i in range(1, len(df)):
        if cond_cross.iat[i]:
            start = max(0, i - MIN_LOOKBACK)
            if (df["close"].iloc[start:i] >= df["lower"].iloc[start:i]).all():
                signals.append(i)
                break  # chỉ lấy tín hiệu đầu tiên
    return signals

def place_full_market_order(symbol: str, qty: float, sl_price: float, tp_price: float):
    """Đặt order thị trường cho đến khi khớp đủ, sau đó set SL/TP"""
    remaining = qty
    while remaining > 0:
        try:
            res = session.place_order(
        category="linear",
        symbol=symbol,
        side=side,
        orderType="Market",
        qty=qty,
        timeInForce="GoodTillCancel",
        reduceOnly=False,
        closeOnTrigger=False,
        takeProfit=tp_price,
        takeProfitTriggerBy="LastPrice",
        stopLoss=sl_price,
        stopLossTriggerBy="LastPrice",
    )
            filled = float(res["result"].get("execQty", 0) or res["result"].get("filled_qty", 0))
            remaining -= filled
            logging.info("Filled %s %s, remaining %s", filled, symbol, remaining)
        except (HTTPError, FailedRequestError) as e:
            logging.error("Error placing market order for %s: %s", symbol, e)
            return
        time.sleep(RATE_LIMIT_DELAY)
    try:
        session.set_trading_stop(
            symbol=symbol,
            take_profit=tp_price,
            stop_loss=sl_price,
            time_in_force="GoodTillCancel"
        )
        logging.info("SL/TP set for %s: SL=%s, TP=%s", symbol, sl_price, tp_price)
    except (HTTPError, FailedRequestError) as e:
        logging.error("Error setting SL/TP for %s: %s", symbol, e)

def main():
    files = sorted(glob.glob(str(history_dir / "*_1h.csv")))
    if not files:
        logging.error("No CSV files found in %s", history_dir)
        return
    for fp in files:
        df = pd.read_csv(fp)
        signals = find_signals(df)
        if not signals:
            continue
        idx = signals[0]
        symbol = Path(fp).stem.replace("_1h", "")
        entry = float(df.at[idx, "close"])
        sl    = float(df.at[idx, "low"])
        tp    = entry * TP_RATIO
        qty, precision = get_qty(symbol, entry)
        if qty <= 0:
            continue
        place_full_market_order(symbol, qty, sl, tp)

if __name__ == "__main__":
    main()
