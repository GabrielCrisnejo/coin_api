import sys
import os
import subprocess
from src.settings import SCHEDULE, COINS_CRON

class CronJobManager:
    """Class for managing CRON jobs related to cryptocurrency data processing."""

    def __init__(self, schedule: str, coins: list, script_path: str = "main.py"):
        self.script_path = os.path.abspath(script_path)
        self.python_path = sys.executable  # Gets the current Python interpreter
        self.schedule = schedule
        self.coins = coins

    def generate_cron_job(self, coin: str) -> str:
        """Generates a CRON job for a given coin with file locking."""
        lock_file = f"/tmp/{coin}_lockfile.lock"
        cron_job_format = "{schedule} flock -n {lock_file} {python_path} {script_path} --coin-id {coin} --start-date $(date +\\%Y-\\%m-\\%d) --end-date $(date +\\%Y-\\%m-\\%d) --store"
        return cron_job_format.format(schedule=self.schedule, lock_file=lock_file, python_path=self.python_path, script_path=self.script_path, coin=coin)

    def get_current_cron_jobs(self) -> list:
        """Fetch the current list of CRON jobs."""
        try:
            cron_jobs = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
            cron_list = cron_jobs.stdout.strip().split("\n") if cron_jobs.stdout.strip() else []
            return cron_list
        except subprocess.CalledProcessError as e:
            print(f"❌ Error fetching CRON jobs: {e}")
            return []

    def add_missing_cron_jobs(self, cron_list: list) -> list:
        """Adds missing CRON jobs to the crontab."""
        cron_jobs = [self.generate_cron_job(coin) for coin in self.coins]
        for job in cron_jobs:
            if job not in cron_list:
                cron_list.append(job)
        return cron_list

    def update_cron_jobs(self, cron_list: list) -> None:
        """Update the crontab with the new CRON jobs."""
        try:
            new_cron_jobs = "\n".join(cron_list) + "\n"
            subprocess.run(["crontab", "-"], input=new_cron_jobs, text=True)
            print("✅ CRON jobs added successfully.")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error updating CRON jobs: {e}")

    def add_cron_jobs(self) -> None:
        """Adds the CRON jobs if they don't already exist."""
        cron_list = self.get_current_cron_jobs()

        # Print current crontab for debugging
        if cron_list:
            print("🔍 Current CRON jobs:\n" + "\n".join(cron_list))
        else:
            print("No existing CRON jobs.")

        cron_jobs = [self.generate_cron_job(coin) for coin in self.coins]

        if all(job in cron_list for job in cron_jobs):
            print("✅ All CRON jobs are already configured.")
            return

        updated_cron_list = self.add_missing_cron_jobs(cron_list)
        self.update_cron_jobs(updated_cron_list)

if __name__ == "__main__":
    cron_job_manager = CronJobManager(schedule=SCHEDULE, coins=COINS_CRON)
    cron_job_manager.add_cron_jobs()
