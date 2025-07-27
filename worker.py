#!/usr/bin/env python3
import os
import subprocess
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

# Đường dẫn các script
HISTORY_DIR = "/data/bybit_full_history"
SCRIPT_57 = "/data/57.py"
SCRIPT_B7 = "/data/b11timeok.py"

os.environ["PYTHONIOENCODING"] = "utf-8"

def job():
    print("🔍 DEBUG: job() bắt đầu chạy.")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n[{timestamp}] === Running BYBIT WORKER JOB ===")
    try:
        files = os.listdir(HISTORY_DIR)
        print(f"📦 Số file trong /data/bybit_full_history: {len(files)}")
    except Exception as e:
        print(f"❌ Không thể truy cập thư mục dữ liệu: {e}")
        return

    print(f"[{timestamp}] === Starting 57.py ===")
    r1 = subprocess.run(["python", SCRIPT_57], capture_output=True, text=True)
    print("📤 57.py output:\n", r1.stdout)
    if r1.stderr:
        print("❌ Error (57.py):", r1.stderr)

    print(f"[{timestamp}] === Starting b11timeok.py ===")
    r2 = subprocess.run(["python", SCRIPT_B7], capture_output=True, text=True)
    print("📤 b11timeok.py output:\n", r2.stdout)
    if r2.stderr:
        print("❌ Error (b11timeok.py):", r2.stderr)

if __name__ == "__main__":
    try:
        print("🧪 Gọi job() lần đầu...")
        job()
    except Exception as e:
        print("❌ Lỗi khi chạy job() lần đầu:", e)

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(job, "cron", minute=1)
    print("🕒 Scheduler started. Job will run every hour at HH:01 UTC.")
    scheduler.start()
