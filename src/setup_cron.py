import os
import subprocess
import sys
from datetime import datetime
from settings import *

# Get absolute paths
SCRIPT_PATH = os.path.abspath("src/fetcher.py")
PYTHON_PATH = sys.executable  # Gets the current Python interpreter

# Generate the CRON jobs for each coin
CRON_JOBS = [
    f"{SCHEDULE} {PYTHON_PATH} {SCRIPT_PATH} --coin-id {coin} --date $(date +\\%Y-\\%m-\\%d)"
    for coin in COINS_CRON
]

def add_cron_jobs():
    """ Adds the CRON jobs if they don't already exist """
    try:
        # Get current crontab list
        cron_jobs = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        cron_list = cron_jobs.stdout.strip().split("\n") if cron_jobs.stdout.strip() else []

        # Print current crontab for debugging
        print("🔍 Current CRON jobs:\n" + "\n".join(cron_list) if cron_list else "No existing CRON jobs.")

        # Check if all CRON jobs already exist
        if all(job in cron_list for job in CRON_JOBS):
            print("✅ All CRON jobs are already configured.")
            return

        # Add missing CRON jobs
        for job in CRON_JOBS:
            if job not in cron_list:
                cron_list.append(job)

        new_cron_jobs = "\n".join(cron_list) + "\n"
        subprocess.run(["crontab", "-"], input=new_cron_jobs, text=True)

        print("✅ CRON jobs added successfully.")

    except Exception as e:
        print(f"❌ Error configuring CRON: {e}")


if __name__ == "__main__":
    add_cron_jobs()
