#!/usr/bin/env python3
import os
import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Đường dẫn đến script cập nhật data và đặt lệnh
SCRIPT_U4 = os.path.join(BASE_DIR, "u4.py")
SCRIPT_T1 = os.path.join(BASE_DIR, "t1.py")

def job():
    print("==== [WORKER] Đang cập nhật dữ liệu (u4.py) ====")
    r1 = subprocess.run(["python", SCRIPT_U4], capture_output=True, text=True)
    print(r1.stdout)
    if r1.stderr:
        print("[Error] u4.py:", r1.stderr)

    print("==== [WORKER] Đang chạy bot đặt lệnh (t1.py) ====")
    r2 = subprocess.run(["python", SCRIPT_T1], capture_output=True, text=True)
    print(r2.stdout)
    if r2.stderr:
        print("[Error] t1.py:", r2.stderr)

if __name__ == "__main__":
    # Chạy job lần đầu ngay khi khởi động
    job()
    # Thiết lập scheduler chạy mỗi giờ
    scheduler = BlockingScheduler()
    scheduler.add_job(job, "cron", minute=1)  # Chạy vào phút 01 mỗi giờ
    print("[WORKER] Scheduler started. Job sẽ chạy mỗi giờ vào phút 01.")
    scheduler.start()
