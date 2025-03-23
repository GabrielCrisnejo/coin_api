import os
import json
import time
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from src.logger import setup_logger
from src.settings import API_URL_TEMPLATE, DATA, SLEEP_TIME, CONCURRENT_REQUESTS, REQUESTS_PER_MINUTE, API_HEADER, API_KEY

# Logger configuration
logger = setup_logger("fetcher")

def fetch_crypto_data(coin_id: str, date: datetime, is_bulk: bool = True) -> None:
    """Fetch historical cryptocurrency data for a specific date.

    Args:
        coin_id (str): The unique identifier of the cryptocurrency.
        date (datetime): The date for which to fetch the historical data.
        is_bulk (bool, optional): A flag indicating if the request is part of bulk processing (default is True).

    Returns:
        None
    """
    formatted_date = date.strftime("%d-%m-%Y")
    url = API_URL_TEMPLATE.format(coin_id=coin_id)
    
    try:
        headers = { f'{API_HEADER}': f'{API_KEY}' }
        response = requests.get(url, params={"date": formatted_date}, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        os.makedirs(DATA, exist_ok=True)
        filename = os.path.join(DATA, f"{coin_id}_{date.strftime('%Y-%m-%d')}.json")
        
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        
        logger.info(f"✅ Data saved to {filename}")
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:
            logger.error(f"❌ Error 429 for {coin_id} on {formatted_date}: Rate limit exceeded")
            logger.info(f"Waiting {SLEEP_TIME} seconds before retrying...")
            time.sleep(SLEEP_TIME)
            # Only retry the request after the delay
            fetch_crypto_data(coin_id, date, is_bulk)  
        else:
            logger.error(f"❌ HTTP error {response.status_code} for {coin_id} on {formatted_date}: {response.text}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"🚨 Request failed for {coin_id} on {formatted_date}: {req_err}")

def process_single_date(coin: str, single_date: datetime) -> None:
    """Process a single day's data for a specific cryptocurrency.

    Args:
        coin (str): The unique identifier of the cryptocurrency.
        single_date (datetime): The specific date to process.

    Returns:
        None
    """
    fetch_crypto_data(coin, single_date, is_bulk=False)

def bulk_reprocess_data(start_date: str, end_date: str, coins: List[str]) -> None:
    """Processes data for a date range using concurrency.

    Args:
        start_date (str): The start date of the range (format: YYYY-MM-DD).
        end_date (str): The end date of the range (format: YYYY-MM-DD).
        coins (List[str]): A list of cryptocurrency coin identifiers.

    Returns:
        None
    """
    logger.info(f"📅 Starting bulk processing from {start_date} to {end_date}")
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end_date_dt - start_date_dt).days + 1
    
    with ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
        futures = []
        request_count = 0  # Request counter
        total_requests = total_days * len(coins)  # Total expected requests

        for n in range(total_days):
            single_date = start_date_dt + timedelta(n)
            for coin in coins:
                # Check if the limit will be hit before making the request
                if request_count % REQUESTS_PER_MINUTE == 0 and request_count != 0:
                    logger.info(f"Limit of {REQUESTS_PER_MINUTE} requests reached. Waiting 60 seconds...")
                    time.sleep(60)  # Enforce API rate limit before sending more requests

                # Now submit the request
                futures.append(executor.submit(fetch_crypto_data, coin, single_date, is_bulk=True))
                request_count += 1
        
        for future in as_completed(futures):
            future.result()

def fetch_single_day_data(coin_id: str, date: str) -> None:
    """Fetch data for a single coin and date.

    Args:
        coin_id (str): The unique identifier of the cryptocurrency.
        date (str): The date to fetch data for (format: YYYY-MM-DD).

    Returns:
        None
    """
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    fetch_crypto_data(coin_id, date_obj, is_bulk=False)
