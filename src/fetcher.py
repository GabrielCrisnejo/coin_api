import os
import json
import time
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from src.logger import setup_logger

# Logger configuration
logger = setup_logger("fetcher")


class CryptoDataFetcher:
    def __init__(self, api_url_template: str, data_directory: str, api_header: str, api_key: str, sleep_time: int):
        self.api_url_template = api_url_template
        self.data_directory = data_directory
        self.api_header = api_header
        self.api_key = api_key
        self.sleep_time = sleep_time

    def fetch_data(self, coin_id: str, date: datetime) -> None:
        """Fetch historical cryptocurrency data for a specific date and save it to a file."""
        formatted_date = date.strftime("%d-%m-%Y")
        url = self.api_url_template.format(coin_id=coin_id)
        
        try:
            headers = {f'{self.api_header}': f'{self.api_key}'}
            response = requests.get(url, params={"date": formatted_date}, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            os.makedirs(self.data_directory, exist_ok=True)
            filename = os.path.join(self.data_directory, f"{coin_id}_{date.strftime('%Y-%m-%d')}.json")
            
            with open(filename, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4)
            
            logger.info(f"✅ Data saved to {filename}")
        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:
                logger.error(f"❌ Error 429 for {coin_id} on {formatted_date}: Rate limit exceeded")
                logger.info(f"Waiting {self.sleep_time} seconds before retrying...")
                time.sleep(self.sleep_time)
                # Retry the request after the delay
                self.fetch_data(coin_id, date)
            else:
                logger.error(f"❌ HTTP error {response.status_code} for {coin_id} on {formatted_date}: {response.text}")
        except requests.exceptions.RequestException as req_err:
            logger.error(f"🚨 Request failed for {coin_id} on {formatted_date}: {req_err}")


class BulkProcessor:
    def __init__(self, fetcher: CryptoDataFetcher, concurrent_requests: int, requests_per_minute: int):
        self.fetcher = fetcher
        self.concurrent_requests = concurrent_requests
        self.requests_per_minute = requests_per_minute

    def process_single_day(self, coin: str, single_date: datetime) -> None:
        """Process a single day's data for a specific cryptocurrency."""
        self.fetcher.fetch_data(coin, single_date)

    def bulk_reprocess_data(self, start_date: str, end_date: str, coins: List[str]) -> None:
        """Processes data for a date range using concurrency."""
        logger.info(f"📅 Starting bulk processing from {start_date} to {end_date}")
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
        total_days = (end_date_dt - start_date_dt).days + 1
        
        with ThreadPoolExecutor(max_workers=self.concurrent_requests) as executor:
            futures = []
            request_count = 0  # Request counter
            total_requests = total_days * len(coins)  # Total expected requests

            for n in range(total_days):
                single_date = start_date_dt + timedelta(n)
                for coin in coins:
                    # Check if the limit will be hit before making the request
                    if request_count % self.requests_per_minute == 0 and request_count != 0:
                        logger.info(f"Limit of {self.requests_per_minute} requests reached. Waiting 60 seconds...")
                        time.sleep(60)  # Enforce API rate limit before sending more requests

                    # Submit the request
                    futures.append(executor.submit(self.fetcher.fetch_data, coin, single_date))
                    request_count += 1
            
            for future in as_completed(futures):
                future.result()

    def fetch_single_day_data(self, coin_id: str, date: str) -> None:
        """Fetch data for a single coin and date."""
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        self.fetcher.fetch_data(coin_id, date_obj)

