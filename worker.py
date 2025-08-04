#!/usr/bin/env python3
import os
import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler

print("=== [DEBUG] worker.py started ===")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SCRIPT_U4 = os.path.join(BASE_DIR, "u4.py")
SCRIPT_T1 = os.path.join(BASE_DIR, "t1.py")

def job():
    print("==== [WORKER] JOB START ====")
    try:
        print("==== [WORKER] Đang cập nhật dữ liệu (u4.py) ====")
        r1 = subprocess.run(["python", SCRIPT_U4], capture_output=True, text=True)
        print(r1.stdout)
        if r1.stderr:
            print("[Error] u4.py:", r1.stderr)
    except Exception as e:
        print("[WORKER][ERROR] Khi chạy u4.py:")
        import traceback
        traceback.print_exc()
        print(f"Exception: {e}")

    try:
        print("==== [WORKER] Đang chạy bot đặt lệnh (t1.py) ====")
        r2 = subprocess.run(["python", SCRIPT_T1], capture_output=True, text=True)
        print(r2.stdout)
        if r2.stderr:
            print("[Error] t1.py:", r2.stderr)
    except Exception as e:
        print("[WORKER][ERROR] Khi chạy t1.py:")
        import traceback
        traceback.print_exc()
        print(f"Exception: {e}")

    print("==== [WORKER] JOB END ====")

if __name__ == "__main__":
    try:
        print("=== [DEBUG] worker.py main starting ===")
        job()
        scheduler = BlockingScheduler()
        scheduler.add_job(job, "cron", minute=0, second=15)
        print("[WORKER] Scheduler started. Job sẽ chạy mỗi giờ vào phút 01.")
        scheduler.start()
    except Exception as e:
        print("=== [ERROR] Exception in worker.py main ===")
        import traceback
        traceback.print_exc()
        print(f"Exception: {e}")
