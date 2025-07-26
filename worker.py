#!/usr/bin/env python3
import os
import subprocess
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

# Directory where CSVs are stored on the persistent disk
HISTORY_DIR = "/data/bybit_full_history"
SCRIPT_57    = "/data/57.py"
SCRIPT_B7    = "/data/b7timeok.py"

# Ensure UTF-8 for subprocess I/O
os.environ["PYTHONIOENCODING"] = "utf-8"

def job():
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{timestamp}] === Starting 57.py ===")
    r1 = subprocess.run(["python", SCRIPT_57], capture_output=True, text=True)
    print(r1.stdout)
    if r1.stderr:
        print("Error (57.py):", r1.stderr)

    print(f"[{timestamp}] === Starting b7timeok.py ===")
    r2 = subprocess.run(["python", SCRIPT_B7], capture_output=True, text=True)
    print(r2.stdout)
    if r2.stderr:
        print("Error (b7timeok.py):", r2.stderr)

if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone="UTC")
    # Schedule: run at minute 1 of every hour
    scheduler.add_job(job, 'cron', minute=1)
    print("Scheduler started. Job will run every hour at HH:01 UTC.")
    scheduler.start()
