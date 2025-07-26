import glob
import pandas as pd
from pathlib import Path
from pybit.unified_trading import HTTP
import numpy as np

# ---- Thư mục dữ liệu ----
history_dir = Path(r"D:\C5_COIN\D1_DU_LIEU\bybit_full_history")

# ---- Bybit API ----
API_KEY = "fbUayzuilSFH13a87H"
API_SECRET = "mwJRm13UYelLHJsL00iyHFX2VPQ6dL4N"
session = HTTP(api_key=API_KEY, api_secret=API_SECRET)
# Đồng bộ thời gian
session.get_server_time()

# ---- Thông số chiến lược ----
MIN_LOOKBACK = 26
TP_RATIO = 1.02
MARGIN = 1.0
LEVERAGE = 10

def get_qty(symbol, entry_price, margin, leverage, session):
    info = session.get_instruments_info(category="linear", symbol=symbol)
    filt = info["result"]["list"][0]["lotSizeFilter"]
    step = float(filt["qtyStep"])
    min_qty = float(filt["minOrderQty"])
    raw = (margin * leverage) / entry_price
    precision = abs(int(np.log10(step))) if step > 0 else 0
    qty = max(raw, min_qty)
    qty = (np.floor(qty / step) * step) if step > 0 else qty
    return str(round(qty, precision))

def find_signals(df):
    df = df.copy()
    df['lower_prev'] = df['lower'].shift(1)
    df['close_prev'] = df['close'].shift(1)
    # Chỉ giữ điều kiện cross-under
    cond_cross = (df['close'] < df['lower']) & (df['close_prev'] >= df['lower_prev'])

    signals = []
    for i in df.index:
        if not cond_cross.iat[i]:
            continue
        start = max(0, i - MIN_LOOKBACK)
        if (df['close'].iloc[start:i] >= df['lower'].iloc[start:i]).all():
            signals.append(i)
    return signals

def place_order(symbol, qty, tp_price, sl_price):
    try:
        return session.place_order(
            symbol=symbol,
            side="Buy",
            order_type="Market",
            qty=qty,
            time_in_force="GoodTillCancel",
            take_profit=tp_price,
            stop_loss=sl_price
        )
    except Exception as e:
        print(f"Order error for {symbol}: {e}")
        return None

def main():
    for fp in glob.glob(str(history_dir / "*_1h.csv")):
        try:
            df = pd.read_csv(fp)
        except pd.errors.EmptyDataError:
            print(f"Skipping empty file: {fp}")
            continue

        # Giữ nguyên index gốc, không reset
        time_col = df.columns[0]
        df[time_col] = pd.to_datetime(df[time_col], utc=True, errors='coerce')

        sig_idxs = find_signals(df)
        if sig_idxs and sig_idxs[-1] == df.index[-1]:
            idx = sig_idxs[-1]
            ts = df.at[idx, time_col]
            entry = df.at[idx, 'close']
            sl = df.at[idx, 'low']
            tp = entry * TP_RATIO
            qty = get_qty(Path(fp).stem, entry, MARGIN, LEVERAGE, session)
            res = place_order(Path(fp).stem, qty, tp, sl)
            print(f"{Path(fp).stem} | {ts} | entry={entry:.4f} | sl={sl:.4f} | tp={tp:.4f} | qty={qty}")
            print("Order response:", res)

if __name__ == "__main__":
    main()
