import os
import subprocess
import sys
from src.settings import SCHEDULE, COINS_CRON

# Constants
SCRIPT_PATH = os.path.abspath("main.py")
PYTHON_PATH = sys.executable  # Gets the current Python interpreter
CRON_JOB_FORMAT = "{schedule} {python_path} {script_path} --coin-id {coin} --start-date $(date +\\%Y-\\%m-\\%d) --end-date $(date +\\%Y-\\%m-\\%d) --store"

# Generate the CRON jobs for each coin
CRON_JOBS = [
    CRON_JOB_FORMAT.format(schedule=SCHEDULE, python_path=PYTHON_PATH, script_path=SCRIPT_PATH, coin=coin)
    for coin in COINS_CRON
]

def get_current_cron_jobs() -> list:
    """Fetch the current list of CRON jobs.

    Returns:
        list: A list of current CRON jobs as strings.
    """
    try:
        cron_jobs = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        cron_list = cron_jobs.stdout.strip().split("\n") if cron_jobs.stdout.strip() else []
        return cron_list
    except subprocess.CalledProcessError as e:
        print(f"❌ Error fetching CRON jobs: {e}")
        return []

def add_missing_cron_jobs(cron_list: list) -> list:
    """Adds missing CRON jobs to the crontab.

    Args:
        cron_list (list): A list of existing CRON jobs.

    Returns:
        list: The updated list of CRON jobs including any missing jobs.
    """
    for job in CRON_JOBS:
        if job not in cron_list:
            cron_list.append(job)
    return cron_list

def update_cron_jobs(cron_list: list) -> None:
    """Update the crontab with the new CRON jobs.

    Args:
        cron_list (list): The updated list of CRON jobs.

    Returns:
        None
    """
    try:
        new_cron_jobs = "\n".join(cron_list) + "\n"
        subprocess.run(["crontab", "-"], input=new_cron_jobs, text=True)
        print("✅ CRON jobs added successfully.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error updating CRON jobs: {e}")

def add_cron_jobs() -> None:
    """Adds the CRON jobs if they don't already exist.

    Returns:
        None
    """
    cron_list = get_current_cron_jobs()

    # Print current crontab for debugging
    if cron_list:
        print("🔍 Current CRON jobs:\n" + "\n".join(cron_list))
    else:
        print("No existing CRON jobs.")

    if all(job in cron_list for job in CRON_JOBS):
        print("✅ All CRON jobs are already configured.")
        return

    updated_cron_list = add_missing_cron_jobs(cron_list)
    update_cron_jobs(updated_cron_list)

if __name__ == "__main__":
    add_cron_jobs()
