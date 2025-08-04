import sys
import asyncio

# Trên Windows, chuyển sang SelectorEventLoop để hài hoà với aiodns
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import os
import aiohttp
import pandas as pd
from datetime import datetime, timedelta, timezone

# ==== CẤU HÌNH ====
contracts_file = r"D:\C5_COIN\B2_BIG_DATA\1_S1_SoLuong_Cap\capcoin.csv"
data_folder = r"D:\C5_COIN\B3_BBRSI\Data1200bar"

interval = "60"  # nến 1h
batch_limit = 200  # theo khuyến nghị Bybit; có thể tăng lên 400 nếu cần
sem_limit = 10  # số symbol xử lý đồng thời
start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)


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
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    return df


# ==== HỖ TRỢ BACKFILL TỪ ĐẦU ====
async def fetch_full_history(session, symbol, category, last_closed):
    parts = []
    end_ts = int(last_closed.timestamp() * 1000)
    while True:
        start_ts = max(0, end_ts - batch_limit * 3600 * 1000)
        dfp = await fetch_ohlc(session, symbol, category, start_ts, end_ts)
        if dfp.empty:
            break
        parts.append(dfp)
        if len(dfp) < batch_limit:
            break
        end_ts = start_ts
    if not parts:
        return pd.DataFrame()
    full = pd.concat(parts, ignore_index=True).drop_duplicates("timestamp")
    full = full.sort_values("timestamp").reset_index(drop=True)
    # Xóa timezone để chỉ ghi datetime naive
    full["timestamp"] = full["timestamp"].dt.tz_localize(None)
    return full


# ==== XỬ LÝ 1 SYMBOL ====
async def process_symbol(symbol, linear_set, spot_set, last_closed, session, sem):
    async with sem:
        # xác định category
        if symbol in linear_set:
            category = "linear"
        elif symbol in spot_set:
            category = "spot"
        else:
            category = "linear"
            print(f"[{symbol}] Unknown category, attempting linear first.")

        fp = os.path.join(data_folder, f"{symbol}_1h.csv")

        # Full backfill nếu chưa có file
        if not os.path.exists(fp):
            print(f"[{symbol}] No file, backfilling full history ({category})...")
            df_full = await fetch_full_history(session, symbol, category, last_closed)
            if df_full.empty and category == "linear":
                print(f"[{symbol}] No data for linear, retrying with spot...")
                df_full = await fetch_full_history(session, symbol, "spot", last_closed)
                category_used = "spot"
            else:
                category_used = category
            if df_full.empty:
                print(f"[{symbol}] ⚠️ No data for both categories, skipping.")
            else:
                os.makedirs(data_folder, exist_ok=True)
                df_full.to_csv(fp, index=False)
                print(
                    f"[{symbol}] ✓ Full backfill saved ({category_used}): {len(df_full)} candles"
                )
            return

        # incremental update
        df_old = pd.read_csv(fp)
        df_old = df_old.iloc[:, :5]
        df_old.columns = ["timestamp", "open", "high", "low", "close"]
        df_old["timestamp"] = pd.to_datetime(df_old["timestamp"], utc=True)

        if df_old.empty:
            print(
                f"[{symbol}] Empty file detected, backfilling full history ({category})..."
            )
            df_full = await fetch_full_history(session, symbol, category, last_closed)
            if df_full.empty and category == "linear":
                print(f"[{symbol}] No data for linear, retrying with spot...")
                df_full = await fetch_full_history(session, symbol, "spot", last_closed)
                category_used = "spot"
            else:
                category_used = category
            if df_full.empty:
                print(f"[{symbol}] ⚠️ No data on retry, skipping.")
            else:
                df_full.to_csv(fp, index=False)
                print(
                    f"[{symbol}] ✓ Backfill saved ({category_used}): {len(df_full)} candles"
                )
            return

        last_ts = df_old["timestamp"].max().to_pydatetime().replace(tzinfo=timezone.utc)
        since = last_ts + timedelta(hours=1)
        if since > last_closed:
            print(f"[{symbol}] ℹ️ Up-to-date ({category}), normalizing file...")
            # Xóa timezone và ghi lại chỉ 5 cột
            df_old["timestamp"] = df_old["timestamp"].dt.tz_localize(None)
            df_old.to_csv(fp, index=False)
            return

        print(f"[{symbol}] Updating from {since} ({category})")
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
            # Xóa timezone trước khi lưu
            df_combined["timestamp"] = df_combined["timestamp"].dt.tz_localize(None)
            df_combined.to_csv(fp, index=False)
            print(f"[{symbol}] ✓ Updated to {last_closed.replace(tzinfo=None)}")


# ==== MAIN ====
async def main():
    os.makedirs(data_folder, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    last_closed = now_utc.replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=1
    )

    with open(contracts_file, "r") as f:
        symbols = [line.strip() for line in f if line.strip()]

    existing = {fn[:-4] for fn in os.listdir(data_folder) if fn.endswith("_1h.csv")}
    to_remove = existing - set(symbols)
    for sym in to_remove:
        try:
            os.remove(os.path.join(data_folder, f"{sym}_1h.csv"))
            print(f"[DEL] Removed old {sym}_1h.csv")
        except:
            pass

    connector = aiohttp.TCPConnector(limit=sem_limit)
    sem = asyncio.Semaphore(sem_limit)
    async with aiohttp.ClientSession(connector=connector) as session:
        linear_set, spot_set = await fetch_symbol_lists(session)
        tasks = [
            process_symbol(sym, linear_set, spot_set, last_closed, session, sem)
            for sym in symbols
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
