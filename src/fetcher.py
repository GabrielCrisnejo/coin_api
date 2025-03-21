import os
import json
import time
import requests
from datetime import datetime, timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.logger import * 
from src.settings import * 

# Logger configuration
logger = setup_logger("fetcher")

def fetch_crypto_data(coin_id, date, is_bulk=True):
    """Fetch historical cryptocurrency data for a specific date."""
    try:
        formatted_date = date.strftime("%d-%m-%Y")
        url = API_URL_TEMPLATE.format(coin_id=coin_id)
        response = requests.get(url, params={"date": formatted_date})

        if response.status_code == 200:
            data = response.json()

            # Determine the correct folder based on is_bulk flag
            folder = DATA_DIR_BULK if is_bulk else DATA_DIR_SINGLE

            # Ensure the folder exists
            os.makedirs(folder, exist_ok=True)

            filename = os.path.join(folder, f"{coin_id}_{date.strftime('%Y-%m-%d')}.json")
            
            with open(filename, "w") as file:
                json.dump(data, file, indent=4)
            
            logger.info(f"✅ Data saved to {filename}")
        elif response.status_code == 429:
            logger.error(f"❌ Error 429 for {coin_id} on {formatted_date}: Rate limit exceeded")
            logger.info(f"Waiting {SLEEP_TIME} seconds before retrying...")
            time.sleep(SLEEP_TIME)
            fetch_crypto_data(coin_id, date, is_bulk)  # Retry request
        else:
            logger.error(f"❌ Error {response.status_code} for {coin_id} on {formatted_date}: {response.text}")
    except requests.RequestException as e:
        logger.error(f"🚨 Request failed for {coin_id} on {formatted_date}: {e}")

def process_single_date(coin, single_date):
    """Process a single day's data for a specific cryptocurrency."""
    fetch_crypto_data(coin, single_date, is_bulk=False)

def bulk_reprocess_data(start_date, end_date, coins):
    """Processes data for a date range using concurrency."""
    logger.info(f"📅 Starting bulk processing from {start_date} to {end_date}")
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    total_days = (end_date - start_date).days + 1
    with ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
        futures = []
        request_count = 0  # Request counter
        # For each date and cryptocurrency, we add the processing task
        for single_date in tqdm((start_date + timedelta(n) for n in range(total_days)), total=total_days, desc="Processing dates"):
            for coin in coins:
                futures.append(executor.submit(fetch_crypto_data, coin, single_date, is_bulk=True))
                request_count += 1

                # If the request limit of 30 is reached, wait for 60 seconds
                if request_count >= REQUESTS_PER_MINUTE:
                    logger.info(f"Limit of {REQUESTS_PER_MINUTE} requests reached. Waiting 60 seconds...")
                    time.sleep(60)  # Wait 1 minute to avoid exceeding the limit
                    request_count = 0  # Reset the request counter

        # Wait for all futures to complete
        for future in as_completed(futures):
            future.result()  # This will raise any exception that occurred in the thread

def fetch_single_day_data(coin_id, date):
    """Fetch data for a single coin and date."""
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    fetch_crypto_data(coin_id, date_obj, is_bulk=False)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Crypto Data Fetcher")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--coin-id", help="Comma-separated list of coin identifiers (e.g., bitcoin,ethereum)", default=None)
    parser.add_argument("--date", help="Date for a single day's data (YYYY-MM-DD)")

    args = parser.parse_args()

    # If no coins are specified, use the default COINS from settings.py
    coins_to_fetch = COINS if not args.coin_id else args.coin_id.split(",")

    if args.start_date and args.end_date:
        # Bulk data processing
        bulk_reprocess_data(args.start_date, args.end_date, coins_to_fetch)
    elif args.coin_id and args.date:
        # Single date data fetch
        fetch_single_day_data(args.coin_id, args.date)
    else:
        today = datetime.today().strftime("%Y-%m-%d")
        bulk_reprocess_data(today, today, coins_to_fetch)
