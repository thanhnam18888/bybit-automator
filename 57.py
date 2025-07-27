import os
import time
import random
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from multiprocessing import Pool

# === Cài đặt ===
folder_path = "/data/bybit_full_history"
interval = "60"
total_limit = 1200  # tổng số nến cần
batch_size = 200    # mỗi lần gọi 200 nến
window = 499        # lookback bars (0..499)
h = 8.0
mult = 3.0
base_url = "https://api.bybit.com/v5/market/kline"

# Precompute Gaussian weights (0..499) for Pine match, denominator fixed
coefs = np.array([np.exp(-(i**2)/(2*h*h)) for i in range(window+1)])
den = coefs.sum()

# Retry/backoff config
max_retries = 3
base_backoff = 2.0  # seconds

def fetch_batch(symbol, start_ts, end_ts, limit):
    params = {
        "symbol": symbol,
        "category": "linear",
        "interval": interval,
        "limit": limit,
        "start": start_ts,
        "end": end_ts,
    }
    try:
        r = requests.get(base_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json().get("result", {}).get("list", [])
        df = pd.DataFrame(data, columns=["timestamp","open","high","low","close","volume","turnover"])
        # CHỈNH SỬA: chuyển về UTC rồi bỏ timezone để thành 'YYYY-MM-DD HH:MM:SS'
        df["timestamp"] = (
            pd.to_datetime(df["timestamp"].astype(np.int64), unit="ms", utc=True)
              .dt.tz_localize(None)
        )
        df[["open","high","low","close","volume","turnover"]] =             df[["open","high","low","close","volume","turnover"]].astype(float)
        return df
    except Exception as e:
        print(f"❌ {symbol} batch fetch error:", e)
        return pd.DataFrame()

def fetch_full_ohlc(symbol):
    now = pd.to_datetime(datetime.utcnow(), utc=True).floor("1h") - pd.Timedelta(hours=1)
    end_ts = int(now.timestamp() * 1000)
    parts = total_limit // batch_size
    dfs = []
    for i in range(parts):
        end = end_ts - i * batch_size * 60*60*1000
        start = end - batch_size * 60*60*1000
        dfp = fetch_batch(symbol, start, end, batch_size)
        if not dfp.empty:
            dfs.append(dfp)
        time.sleep(0.2)
    if dfs:
        df_all = pd.concat(dfs).drop_duplicates("timestamp")                             .sort_values("timestamp")                             .reset_index(drop=True)
        return df_all, now
    return pd.DataFrame(), None

def safe_fetch(symbol):
    last_df, last_now = pd.DataFrame(), None
    for attempt in range(1, max_retries+1):
        df, now = fetch_full_ohlc(symbol)
        if not df.empty and len(df) >= total_limit:
            return df, now
        wait = base_backoff * (2**(attempt-1)) * random.uniform(0.8,1.2)
        print(f"⚠️ {symbol}: chỉ fetch được {len(df)} bars, retry {attempt}/{max_retries} after {wait:.1f}s")
        time.sleep(wait)
    return last_df, last_now

def calc_nwe_full(df):
    src = df["close"].values
    n = len(src)
    nwe = np.full(n, np.nan)
    # compute endpoint out[t] = Σ(src[t-j]*coefs[j]) / Σ coefs
    for t in range(n):
        maxj = min(t, window)
        s = sum(src[t-j] * coefs[j] for j in range(maxj+1))
        denom = coefs[:maxj+1].sum()
        nwe[t] = s/denom if denom>0 else np.nan
    # compute rolling MAE on abs(diff)
    abs_diff = np.abs(src - nwe)
    mae = pd.Series(abs_diff).rolling(window+1, min_periods=1).mean().values * mult
    upper = nwe + mae
    lower = nwe - mae
    df["nwe"]   = nwe
    df["mae"]   = mae
    df["upper"] = upper
    df["lower"] = lower
    return df

def process_file(file):
    if not file.endswith("_1h.csv"):
        return
    symbol = file.replace("_1h.csv","")
    path = os.path.join(folder_path, file)
    df, now = safe_fetch(symbol)
    if df.empty:
        print("🗑️",symbol,"fetch failed → xóa file")
        os.remove(path); return
    expected_start = now - pd.Timedelta(hours=total_limit)
    if len(df) < total_limit:
        if df["timestamp"].iloc[0] > expected_start:
            print("🗑️",symbol,"không đủ bars → xóa file")
            os.remove(path); return
        else:
            print("⚠️",symbol,"thiếu bars, giữ lại")
            return
    df = calc_nwe_full(df)
    if os.path.exists(path):
        os.remove(path)
    # Xuất CSV: timestamp giờ đã ở dạng YYYY-MM-DD HH:MM:SS
    df.to_csv(path, index=False)
    print("✅",symbol,"cập nhật xong",len(df),"bars")

if __name__ == "__main__":
    files = os.listdir(folder_path)
    with Pool(processes=30) as pool:
        pool.map(process_file, files)
