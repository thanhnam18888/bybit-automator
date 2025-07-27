#!/usr/bin/env python3
import os
import time
import random
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from multiprocessing import Pool

# === Cài đặt ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
folder_path = os.path.join(BASE_DIR, "bybit_full_history")
interval = "60"
total_limit = 1200  # tổng số nến cần
batch_size = 200    # mỗi lần gọi 200 nến
window = 499        # lookback bars (0..499)
h = 8.0

# ... (các hàm hỗ trợ: calc_nwe_full, process_file, etc.) ...

def process_file(filename):
    symbol = filename.replace("_1h.csv", "")
    path = os.path.join(folder_path, filename)
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
    except Exception as e:
        print(f"❌ Error đọc file {filename}: {e}", flush=True)
        return

    # (logic fetch và tính toán)
    # ...
    # Sau khi tính df đầy đủ bars
    if os.path.exists(path):
        os.remove(path)
    df.to_csv(path, index=False)
    print("✅", symbol, "cập nhật xong", len(df), "bars", flush=True)

if __name__ == "__main__":
    # Liệt kê file trong thư mục
    try:
        files = os.listdir(folder_path)
    except Exception as e:
        print(f"❌ Không thể truy cập thư mục dữ liệu: {e}", flush=True)
        exit(1)
    if not files:
        print(f"⚠️ Không tìm thấy file CSV trong {folder_path}", flush=True)
    else:
        print(f"🔍 57.py đang chạy với thư mục {folder_path}, tổng files: {len(files)}", flush=True)

    # Dùng Pool đa tiến trình
    with Pool(processes=5) as pool:
        pool.map(process_file, files)
