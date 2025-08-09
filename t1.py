import os
import glob
import pandas as pd
import ta
import numpy as np
import time
import logging
import json
from pybit.unified_trading import HTTP


def ensure_leverage(symbol, lev):
    """Set leverage for both long/short sides on Bybit v5 before placing orders."""
    try:
        session.set_leverage(
            category="linear",
            symbol=symbol,
            buyLeverage=str(lev),
            sellLeverage=str(lev),
        )
    except Exception as e:
        logging.warning(f"Không set leverage {symbol}: {e}")


# ==== CẤU HÌNH ====
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
RECV_WINDOW = 60000
DATA_FOLDER = "/data/Data1200bar"
MAX_OPEN = 100
MARGIN = 50
LEVERAGE = 4
# Các tỷ lệ cũ (không dùng nữa), giữ nguyên để không phá cấu trúc
TP_RATIO = 0.99
SL_RATIO = 1.02
RSI_LEN = 14
RSI_OB = 80
RSI_OS = 20
BB_LEN = 20
STDDEV = 2.0
EMA_LEN = 200
WAIT_BARS = 5
ORDER_LOG = "/data/active_orders.json"

# ==== THAM SỐ TP/SL MỚI (theo yêu cầu) ====
TP_BASE_PCT = 0.02  # +2%
TP_OFFSET = 0.003  # +0.3%  => tổng +2.3%
SL_BASE_PCT = 0.037  # -3.7%
SL_OFFSET = 0.003  # -0.3%  => tổng -4.0% khi đặt limit "xấu" hơn trigger

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


def get_price_tick(symbol):
    """Lấy tick size & precision để làm tròn giá Limit theo quy định sàn."""
    info = session.get_instruments_info(category="linear", symbol=symbol)
    pf = info["result"]["list"][0]["priceFilter"]
    tick = float(pf["tickSize"])
    precision = abs(int(np.log10(tick))) if tick < 1 and tick > 0 else 0
    return tick, precision


def round_price(price, tick, precision):
    if tick <= 0:
        return price
    return round(round(price / tick) * tick, precision)


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
    # Bảo đảm leverage đã được set trước khi đặt lệnh
    try:
        ensure_leverage(symbol, LEVERAGE)
    except Exception:
        pass
    if len(active_orders.get(symbol, [])) >= MAX_OPEN:
        print(f"[T1] {symbol}: ĐÃ ĐỦ {MAX_OPEN} LỆNH đang mở, không vào lệnh mới.")
        return
    # Side mapping (outside MAX_OPEN block)
    if direction == "long":
        side = "Buy"
        entered_is_short = False
        close_side = "Sell"
    elif direction == "short":
        side = "Sell"
        entered_is_short = True
        close_side = "Buy"
    else:
        return

        print(f"[T1] {symbol}: VÀO LỆNH {direction.upper()} MARKET | qty={qty}")
    try:
        # 1) Vào lệnh MARKET
        order = session.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=f"{qty}",
            reduceOnly=False,
            recv_window=RECV_WINDOW,
        )
        order_id = order.get("result", {}).get("orderId")
        if order_id:
            active_orders.setdefault(symbol, []).append(order_id)
            save_active_orders(active_orders)

        # 2) Lấy giá entry thực tế
        real_entry = get_entry_price(order_id, symbol)
        if not real_entry:
            real_entry = entry_price

        # 3) Tính TP/SL theo yêu cầu mới
        # LONG: TP = +2.3%, SL trigger = -3.7%, SL limit = -4.0%
        # SHORT: TP = -2.3%, SL trigger = +3.7%, SL limit = +4.0%
        if entered_is_short:
            tp_raw = real_entry * (1 - (TP_BASE_PCT + TP_OFFSET))  # xuống 2.3%
            sl_trig_raw = real_entry * (1 + SL_BASE_PCT)  # lên 3.7%
            sl_lim_raw = real_entry * (1 + SL_BASE_PCT + SL_OFFSET)  # lên 4.0%
            sl_trigger_dir = 1  # giá RISES ABOVE kích hoạt
        else:
            tp_raw = real_entry * (1 + (TP_BASE_PCT + TP_OFFSET))  # lên 2.3%
            sl_trig_raw = real_entry * (1 - SL_BASE_PCT)  # xuống 3.7%
            sl_lim_raw = real_entry * (1 - SL_BASE_PCT - SL_OFFSET)  # xuống 4.0%
            sl_trigger_dir = 2  # giá FALLS BELOW kích hoạt

        # Làm tròn theo tick
        tick, prec = get_price_tick(symbol)
        tp_price = round_price(tp_raw, tick, prec)
        sl_trigger = round_price(sl_trig_raw, tick, prec)
        sl_limit = round_price(sl_lim_raw, tick, prec)

        print(
            f"[T1] {symbol}: Đặt TP LIMIT tại {tp_price:.{prec}f} (+{(TP_BASE_PCT+TP_OFFSET)*100:.2f}%), "
            f"SL STOP-LIMIT trigger={sl_trigger:.{prec}f}, limit={sl_limit:.{prec}f} (-{(SL_BASE_PCT+SL_OFFSET)*100:.2f}% tổng)"
        )

        # 4) Đặt TP LIMIT ReduceOnly (không trigger)
        tp_order = session.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Limit",
            price=str(tp_price),
            qty=f"{qty}",
            reduceOnly=True,
            timeInForce="GTC",
            recv_window=RECV_WINDOW,
        )

        # 5) Đặt SL STOP-LIMIT ReduceOnly (Limit + triggerPrice)
        sl_order = session.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Limit",
            price=str(sl_limit),
            triggerPrice=str(sl_trigger),
            triggerBy="LastPrice",
            triggerDirection=sl_trigger_dir,
            qty=f"{qty}",
            reduceOnly=True,
            timeInForce="GTC",
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

            # Invert LONG -> SHORT as requested
            if direction == "long":
                direction = "short"
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
