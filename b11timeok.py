import glob
import pandas as pd
from pathlib import Path
from pybit.unified_trading import HTTP
import numpy as np
import time
import logging

# ---- Debug logging (optional) ----
logging.basicConfig(level=logging.INFO)

# ---- Thư mục dữ liệu ----
history_dir = Path(r"D:/C5_COIN/D1_DU_LIEU/bybit_full_history")

# ---- Bybit API cho MAINNET ----
API_KEY      = "fbUayzuilSFH13a87H"
API_SECRET   = "mwJRm13UYelLHJsL00iyHFX2AjX2VPX6dL4N"
RECV_WINDOW  = 60000
session      = HTTP(api_key=API_KEY, api_secret=API_SECRET, recv_window=RECV_WINDOW)
session.get_server_time()

# ---- Thông số chiến lược ----
MIN_LOOKBACK = 26
TP_RATIO     = 1.02
MARGIN       = 1.0
LEVERAGE     = 10

def get_qty(symbol, entry_price):
    info = session.get_instruments_info(category="linear", symbol=symbol)
    filt = info["result"]["list"][0]["lotSizeFilter"]
    step = float(filt["qtyStep"])
    min_qty = float(filt["minOrderQty"])
    raw = (MARGIN * LEVERAGE) / entry_price
    qty = max(min_qty, np.floor(raw / step) * step) if step > 0 else raw
    precision = abs(int(np.log10(step))) if step < 1 and step > 0 else 0
    return qty, precision

def find_signals(df):
    df = df.copy()
    df['lower_prev'] = df['lower'].shift(1)
    df['close_prev'] = df['close'].shift(1)
    cond_cross = (df['close'] < df['lower']) & (df['close_prev'] >= df['lower_prev'])
    signals = []
    for i in df.index:
        if not cond_cross.iat[i]:
            continue
        start = max(0, i - MIN_LOOKBACK)
        if (df['close'].iloc[start:i] >= df['lower'].iloc[start:i]).all():
            signals.append(i)
    return signals

def place_full_market_order(symbol, qty, sl_price, tp_price, precision):
    total_filled = 0.0
    remaining = qty
    while remaining > 0:
        logging.info(f"Placing market for remaining {remaining:.{precision}f} {symbol}")
        order = session.place_order(
            category="linear",
            symbol=symbol,
            side="Buy",
            orderType="Market",
            qty=f"{remaining:.{precision}f}",
            timeInForce="GoodTillCancel",
            takeProfit=str(tp_price),
            stopLoss=str(sl_price),
            recv_window=RECV_WINDOW
        )
        result = order.get("result", {})
        filled = float(result.get("cumExecQty", remaining))
        total_filled += filled
        remaining = qty - total_filled
        if filled == 0:
            logging.warning("No fill in this attempt, stopping.")
            break
        time.sleep(0.5)
    logging.info(f"Total filled: {total_filled:.{precision}f}/{qty:.{precision}f}")
    return total_filled

def main():
    for fp in glob.glob(str(history_dir / "*_1h.csv")):
        try:
            df = pd.read_csv(fp)
        except pd.errors.EmptyDataError:
            logging.warning(f"Skipping empty file: {fp}")
            continue

        time_col = df.columns[0]
        df[time_col] = pd.to_datetime(df[time_col], utc=True, errors='coerce')

        sig_idxs = find_signals(df)
        if sig_idxs and sig_idxs[-1] == df.index[-1]:
            idx = sig_idxs[-1]
            ts = df.at[idx, time_col]
            entry_price = df.at[idx, 'close']
            sl_price = df.at[idx, 'low']
            tp_price = entry_price * TP_RATIO

            symbol = Path(fp).stem.split('_')[0]
            qty, precision = get_qty(symbol, entry_price)
            logging.info(f"{symbol} | {ts} | entry={entry_price:.4f} | sl={sl_price:.4f} | tp={tp_price:.4f} | qty={qty:.{precision}f}")
            place_full_market_order(symbol, qty, sl_price, tp_price, precision)

if __name__ == "__main__":
    main()
