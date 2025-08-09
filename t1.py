import os
import glob
import pandas as pd
import ta
import numpy as np
import time
import logging
import json
from pybit.unified_trading import HTTP

# ==== CẤU HÌNH ====
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
RECV_WINDOW = 60000
DATA_FOLDER = "/data/Data1200bar"
MAX_OPEN = 100
MARGIN = 50
LEVERAGE = 2
TP_RATIO = 0.957
SL_RATIO = 1.077
RSI_LEN = 14
RSI_OB = 80
RSI_OS = 20
BB_LEN = 20
STDDEV = 2.0
EMA_LEN = 200
WAIT_BARS = 5
ORDER_LOG = "/data/active_orders.json"

logging.basicConfig(level=logging.INFO)

print("[T1] Kết nối Bybit...")
session = HTTP(api_key=API_KEY, api_secret=API_SECRET, recv_window=RECV_WINDOW)
try:
    session.get_server_time()
    print("[T1] Kết nối Bybit thành công!")
except Exception as e:
    print("[T1] Lỗi kết nối Bybit:", e)
    exit(1)


def load_active_orders():
    if os.path.exists(ORDER_LOG):
        with open(ORDER_LOG, "r") as f:
            return json.load(f)
    return {}


def save_active_orders(active_orders):
    with open(ORDER_LOG, "w") as f:
        json.dump(active_orders, f)


def cleanup_closed_orders(symbol, active_orders):
    open_order_ids = []
    for order_id in active_orders.get(symbol, []):
        try:
            info = session.get_order_history(
                category="linear", symbol=symbol, orderId=order_id
            )
            orders = info.get("result", {}).get("list", [])
            status = orders[0]["orderStatus"] if orders else "Unknown"
            if status in ["Filled", "Cancelled", "Rejected"]:
                continue
            open_order_ids.append(order_id)
        except Exception as e:
            logging.warning(
                f"Không kiểm tra được trạng thái order {order_id} của {symbol}: {e}"
            )
    active_orders[symbol] = open_order_ids
    save_active_orders(active_orders)
    return len(open_order_ids)


def get_qty(symbol, entry_price):
    info = session.get_instruments_info(category="linear", symbol=symbol)
    filt = info["result"]["list"][0]["lotSizeFilter"]
    step = float(filt["qtyStep"])
    min_qty = float(filt["minOrderQty"])
    raw = (MARGIN * LEVERAGE) / entry_price
    qty = max(min_qty, np.floor(raw / step) * step) if step > 0 else raw
    precision = abs(int(np.log10(step))) if step < 1 and step > 0 else 0
    return qty, precision


def calc_signals(df):
    if len(df) < EMA_LEN + 1:
        return None
    df["rsi"] = ta.momentum.RSIIndicator(df["close"], window=RSI_LEN).rsi()
    bb = ta.volatility.BollingerBands(df["close"], window=BB_LEN, window_dev=STDDEV)
    df["upper"] = bb.bollinger_hband()
    df["lower"] = bb.bollinger_lband()
    df["ema"] = ta.trend.EMAIndicator(df["close"], window=EMA_LEN).ema_indicator()
    cond_long = (
        (df["close"] < df["lower"]) & (df["rsi"] < RSI_OS) & (df["close"] < df["ema"])
    )
    cond_short = (
        (df["close"] > df["upper"]) & (df["rsi"] > RSI_OB) & (df["close"] > df["ema"])
    )
    can_long = cond_long.iat[-1] and not cond_long.iloc[-WAIT_BARS:-1].any()
    can_short = cond_short.iat[-1] and not cond_short.iloc[-WAIT_BARS:-1].any()
    if can_long:
        return "long"
    elif can_short:
        return "short"
    return None


def get_entry_price(order_id, symbol):
    for _ in range(10):
        try:
            info = session.get_order_history(
                category="linear", symbol=symbol, orderId=order_id
            )
            orders = info.get("result", {}).get("list", [])
            if orders and orders[0].get("avgPrice"):
                return float(orders[0]["avgPrice"])
        except Exception as e:
            logging.warning(f"Không lấy được giá entry cho order {order_id}: {e}")
        time.sleep(1)
    return None


def check_position(symbol, direction):
    try:
        info = session.get_positions(category="linear", symbol=symbol)
        positions = info.get("result", {}).get("list", [])
        if not positions:
            return 0.0
        if direction == "long":
            return float(positions[0].get("size", 0))
        else:  # short
            return -float(positions[0].get("size", 0))
    except Exception as e:
        logging.warning(f"Lỗi kiểm tra position {symbol}: {e}")
        return 0.0


def place_market_order_with_tp_sl(symbol, qty, entry_price, direction, active_orders):
    if len(active_orders.get(symbol, [])) >= MAX_OPEN:
        print(f"[T1] {symbol}: ĐÃ ĐỦ {MAX_OPEN} LỆNH đang mở, không vào lệnh mới.")
        return

    if direction == "long":
        side = "Sell"
        close_side = "Buy"
    elif direction == "short":
        side = "Buy"
        close_side = "Sell"
    else:
        return

    print(f"[T1] {symbol}: VÀO LỆNH {direction.upper()} MARKET | qty={qty}")
    try:
        # 1. Đặt lệnh market vào lệnh
        order = session.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=f"{qty}",
            leverage=LEVERAGE,
            reduceOnly=False,
            recv_window=RECV_WINDOW,
        )
        order_id = order.get("result", {}).get("orderId")
        if order_id:
            active_orders.setdefault(symbol, []).append(order_id)
            save_active_orders(active_orders)

        # 2. Lấy giá entry thực tế sau khi market fill
        real_entry = get_entry_price(order_id, symbol)
        if not real_entry:
            real_entry = entry_price

        # Xác định vị thế THỰC TẾ dựa vào 'side' đã vào lệnh
        entered_is_short = side == "Sell"

        if entered_is_short:
            # SHORT: TP dưới entry, SL trên entry
            tp_price = real_entry * TP_RATIO
            sl_price = real_entry * SL_RATIO
            tp_trigger_dir = 2  # giá <= tp_price thì chốt lời
            sl_trigger_dir = 1  # giá >= sl_price thì cắt lỗ
        else:
            # LONG: TP trên entry, SL dưới entry
            tp_price = real_entry / TP_RATIO
            sl_price = real_entry / SL_RATIO
            tp_trigger_dir = 1  # giá >= tp_price thì chốt lời
            sl_trigger_dir = 2  # giá <= sl_price thì cắt lỗ

        tp_percent = abs(tp_price - real_entry) / real_entry * 100.0
        sl_percent = abs(sl_price - real_entry) / real_entry * 100.0

        print(
            f"[T1] {symbol}: Đặt MARKET TP tại {tp_price:.4f} (~{tp_percent:.2f}%), "
            f"MARKET SL tại {sl_price:.4f} (~{sl_percent:.2f}%)"
        )

        # 3. Đặt TP/SL bằng conditional market order (chỉ để đóng vị thế)
        tp_order = session.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=f"{qty}",
            triggerDirection=tp_trigger_dir,
            triggerPrice=str(tp_price),
            reduceOnly=True,
            closeOnTrigger=True,
            recv_window=RECV_WINDOW,
        )

        sl_order = session.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=f"{qty}",
            triggerDirection=sl_trigger_dir,
            triggerPrice=str(sl_price),
            reduceOnly=True,
            closeOnTrigger=True,
            recv_window=RECV_WINDOW,
        )

        logging.info(f"[T1] TP resp: {tp_order}")
        logging.info(f"[T1] SL resp: {sl_order}")
    except Exception as e:
        logging.warning(f"Lỗi đặt lệnh {symbol}: {e}")
        return None


def main():
    active_orders = load_active_orders()
    csv_files = glob.glob(os.path.join(DATA_FOLDER, "*_1h.csv"))
    print(f"[T1] Tổng số file dữ liệu: {len(csv_files)}")
    n_checked, n_signal, n_no_signal, n_error = 0, 0, 0, 0
    for fp in csv_files:
        try:
            symbol = os.path.basename(fp).split("_")[0]
            df = pd.read_csv(fp)
            if not {"open", "high", "low", "close", "volume"}.issubset(df.columns):
                df.columns = ["timestamp", "open", "high", "low", "close", "volume"][
                    : df.shape[1]
                ]
            if len(df) < EMA_LEN + 1:
                continue
            num_open = cleanup_closed_orders(symbol, active_orders)
            direction = calc_signals(df)
            n_checked += 1
            if direction:
                n_signal += 1
                print(
                    f"[T1] {symbol}: Có tín hiệu '{direction.upper()}'. Số lệnh đang mở: {num_open}"
                )
                if num_open < MAX_OPEN:
                    entry_price = df["close"].iat[-1]
                    qty, precision = get_qty(symbol, entry_price)
                    qty = round(qty, precision)
                    if qty > 0:
                        place_market_order_with_tp_sl(
                            symbol, qty, entry_price, direction, active_orders
                        )
                    else:
                        print(f"[T1] {symbol}: Không vào lệnh do qty=0")
                else:
                    print(
                        f"[T1] {symbol}: ĐÃ ĐỦ {MAX_OPEN} LỆNH đang mở, không vào lệnh mới."
                    )
            else:
                n_no_signal += 1
        except Exception as e:
            logging.warning(f"[T1] Lỗi xử lý {fp}: {e}")
            n_error += 1
    print(
        f"[T1] Tổng kết: {n_checked} symbol, {n_signal} có tín hiệu, {n_no_signal} không có tín hiệu, {n_error} lỗi."
    )


if __name__ == "__main__":
    main()
