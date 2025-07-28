#!/usr/bin/env python3
import os
import subprocess
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler

# Thư mục chứa file CSV P2P history (thư mục bybit_full_history trong repo)
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
HISTORY_DIR = os.path.join(BASE_DIR, "bybit_full_history")

# Đường dẫn đến các script
SCRIPT_57 = os.path.join(BASE_DIR, "57.py")
SCRIPT_B7 = os.path.join(BASE_DIR, "b11timeok.py")

# Đảm bảo đầu ra Unicode
os.environ["PYTHONIOENCODING"] = "utf-8"

def job():
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{timestamp}] === Running BYBIT WORKER JOB ===", flush=True)

    try:
        files = os.listdir(HISTORY_DIR)
        print(f"📦 Số file trong {HISTORY_DIR}: {len(files)}", flush=True)
    except Exception as e:
        print(f"❌ Không thể truy cập thư mục dữ liệu: {e}", flush=True)
        return

    # Chạy script 57.py unbuffered
    print(f"[{timestamp}] === Starting 57.py ===", flush=True)
    r1 = subprocess.run(["python", "-u", SCRIPT_57], capture_output=True, text=True)
    print("📤 57.py output:\n", r1.stdout, flush=True)
    if r1.stderr:
        print("❌ Error (57.py):", r1.stderr, flush=True)

    # Chạy script b11timeok.py unbuffered với log rõ ràng
    print(f"[{timestamp}] === Starting b11timeok.py ===", flush=True)
    print(f"[{timestamp}] --- Begin b11timeok.py execution ---", flush=True)
    r2 = subprocess.run(["python", "-u", SCRIPT_B7], capture_output=True, text=True)
    print("📤 b11timeok.py output:\n", r2.stdout, flush=True)
    if r2.stderr:
        print("❌ Error (b11timeok.py):", r2.stderr, flush=True)
    print(f"[{timestamp}] --- End b11timeok.py execution ---", flush=True)

if __name__ == "__main__":
    # Chạy job lần đầu ngay khi khởi động
    print("🧪 Gọi job() lần đầu...", flush=True)
    job()

    # Thiết lập scheduler chạy mỗi giờ vào phút 01 UTC
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(job, "cron", minute=1)
    print("🕒 Scheduler started. Job sẽ chạy mỗi giờ vào phút 01 UTC.", flush=True)
    scheduler.start()
