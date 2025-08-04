import sys
import asyncio

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import os
import aiohttp
import pandas as pd
from datetime import datetime, timedelta, timezone

# ==== CẤU HÌNH ====
contracts_file = "capcoin.csv"
data_folder = "/data/Data1200bar"
interval = "60"  # nến 1h
batch_limit = 200
sem_limit = 30   # Tăng đồng thời, tuỳ Bybit limit (thử 30, test, có thể lên 50)
MAX_BARS = 1200

# ==== HỖ TRỢ LẤY LIST SYMBOL VỚI PAGINATION ====
async def fetch_instrument_set(session, category):
    url = "https://api.bybit.com/v5/market/instruments-info"
    cursor = None
    symbols = set()
    while True:
        params = {"category": category, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        async with session.get(url, params=params) as resp:
            js = await resp.json()
        lst = js["result"].get("list", [])
        symbols |= {item["symbol"] for item in lst}
        cursor = js["result"].get("nextPageCursor")
        if not cursor:
            break
    return symbols

async def fetch_symbol_lists(session):
    linear_set = await fetch_instrument_set(session, "linear")
    spot_set = await fetch_instrument_set(session, "spot")
    return linear_set, spot_set

# ==== HỖ TRỢ LẤY OHLC BATCH ====
async def fetch_ohlc(session, symbol, category, start_ms, end_ms):
    url = "https://api.bybit.com/v5/market/kline"
    params = {
        "symbol": symbol,
        "category": category,
        "interval": interval,
        "limit": batch_limit,
        "start": start_ms,
        "end": end_ms,
    }
    async with session.get(url, params=params) as resp:
        js = await resp.json()
    rows = js["result"].get("list", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).iloc[:, :5]
    df.columns = ["timestamp", "open", "high", "low", "close"]
    # Fix FutureWarning: ép timestamp về numeric trước khi to_datetime
    df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp"]), unit="ms", utc=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    return df

# ==== HỖ TRỢ BACKFILL CHỈ LẤY 1200 NẾN CUỐI ====
async def fetch_full_history(session, symbol, category, last_closed):
    parts = []
    start_time = last_closed - timedelta(hours=MAX_BARS-1)
    start_ts = int(start_time.timestamp() * 1000)
    end_ts = int(last_closed.timestamp() * 1000)
    total = 0
    while start_ts < end_ts and total < MAX_BARS:
        fetch_end_ts = min(end_ts, start_ts + batch_limit * 3600 * 1000)
        dfp = await fetch_ohlc(session, symbol, category, start_ts, fetch_end_ts)
        if dfp.empty:
            break
        parts.append(dfp)
        count = len(dfp)
        total += count
        start_ts = int(dfp["timestamp"].max().timestamp() * 1000) + 3600 * 1000
        if count < batch_limit:
            break
    if not parts:
        return pd.DataFrame()
    full = pd.concat(parts, ignore_index=True).drop_duplicates("timestamp")
    full = full.sort_values("timestamp").reset_index(drop=True)
    if len(full) > MAX_BARS:
        full = full.iloc[-MAX_BARS:]
    full["timestamp"] = full["timestamp"].dt.tz_localize(None)
    return full

# ==== XỬ LÝ 1 SYMBOL (LOG GỌN, BÁO LỖI 1 LẦN) ====
async def process_symbol(symbol, linear_set, spot_set, last_closed, session, sem, summary):
    async with sem:
        try:
            if symbol in linear_set:
                category = "linear"
            elif symbol in spot_set:
                category = "spot"
            else:
                category = "linear"

            fp = os.path.join(data_folder, f"{symbol}_1h.csv")

            # Full backfill nếu chưa có file
            if not os.path.exists(fp):
                df_full = await fetch_full_history(session, symbol, category, last_closed)
                if df_full.empty and category == "linear":
                    df_full = await fetch_full_history(session, symbol, "spot", last_closed)
                    category_used = "spot"
                else:
                    category_used = category
                if df_full.empty:
                    summary["error"].add(f"{symbol}: No data for both categories")
                else:
                    os.makedirs(data_folder, exist_ok=True)
                    if len(df_full) > MAX_BARS:
                        df_full = df_full.iloc[-MAX_BARS:]
                    df_full.to_csv(fp, index=False)
                    summary["updated"] += 1
                return

            # incremental update
            df_old = pd.read_csv(fp)
            df_old = df_old.iloc[:, :5]
            df_old.columns = ["timestamp", "open", "high", "low", "close"]
            df_old["timestamp"] = pd.to_datetime(df_old["timestamp"], utc=True)

            if df_old.empty:
                df_full = await fetch_full_history(session, symbol, category, last_closed)
                if df_full.empty and category == "linear":
                    df_full = await fetch_full_history(session, symbol, "spot", last_closed)
                    category_used = "spot"
                else:
                    category_used = category
                if df_full.empty:
                    summary["error"].add(f"{symbol}: No data on retry")
                else:
                    if len(df_full) > MAX_BARS:
                        df_full = df_full.iloc[-MAX_BARS:]
                    df_full.to_csv(fp, index=False)
                    summary["updated"] += 1
                return

            last_ts = df_old["timestamp"].max().to_pydatetime().replace(tzinfo=timezone.utc)
            since = last_ts + timedelta(hours=1)
            if since > last_closed:
                df_old["timestamp"] = df_old["timestamp"].dt.tz_localize(None)
                if len(df_old) > MAX_BARS:
                    df_old = df_old.iloc[-MAX_BARS:]
                df_old.to_csv(fp, index=False)
                summary["unchanged"] += 1
                return

            parts_new = []
            current = since
            while current <= last_closed:
                start_ms = int(current.timestamp() * 1000)
                end_time = min(current + timedelta(hours=batch_limit), last_closed)
                end_ms = int(end_time.timestamp() * 1000)
                dfp = await fetch_ohlc(session, symbol, category, start_ms, end_ms)
                if dfp.empty:
                    break
                parts_new.append(dfp)
                current = end_time + timedelta(milliseconds=1)

            if parts_new:
                df_new = pd.concat(parts_new, ignore_index=True)
                df_combined = pd.concat([df_old, df_new], ignore_index=True)
                df_combined = (
                    df_combined.drop_duplicates("timestamp")
                    .sort_values("timestamp")
                    .reset_index(drop=True)
                )
                if len(df_combined) > MAX_BARS:
                    df_combined = df_combined.iloc[-MAX_BARS:]
                df_combined["timestamp"] = df_combined["timestamp"].dt.tz_localize(None)
                df_combined.to_csv(fp, index=False)
                summary["updated"] += 1
            else:
                summary["unchanged"] += 1

        except Exception as e:
            summary["error"].add(f"{symbol}: {str(e)}")

# ==== MAIN ====
async def main():
    os.makedirs(data_folder, exist_ok=True)
    now_utc = datetime.now(timezone.utc)
    last_closed = now_utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

    with open(contracts_file, "r") as f:
        symbols = [line.strip() for line in f if line.strip()]

    existing = {fn[:-4] for fn in os.listdir(data_folder) if fn.endswith("_1h.csv")}
    to_remove = existing - set(symbols)
    for sym in to_remove:
        try:
            os.remove(os.path.join(data_folder, f"{sym}_1h.csv"))
        except:
            pass

    connector = aiohttp.TCPConnector(limit=sem_limit)
    sem = asyncio.Semaphore(sem_limit)
    summary = {"updated": 0, "unchanged": 0, "error": set()}
    async with aiohttp.ClientSession(connector=connector) as session:
        linear_set, spot_set = await fetch_symbol_lists(session)
        tasks = [
            process_symbol(sym, linear_set, spot_set, last_closed, session, sem, summary)
            for sym in symbols
        ]
        await asyncio.gather(*tasks)

    print(f"=== Tổng kết: ===")
    print(f"Số symbol cập nhật mới: {summary['updated']}")
    print(f"Số symbol đã đủ data, không cần update: {summary['unchanged']}")
    if summary["error"]:
        print("Các lỗi gặp phải (mỗi lỗi chỉ báo 1 lần):")
        for err in summary["error"]:
            print("  ", err)
    else:
        print("Không gặp lỗi nào.")

if __name__ == "__main__":
    asyncio.run(main())
