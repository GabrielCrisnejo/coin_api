import os
import sys
import argparse
from datetime import datetime, timedelta
from src.fetcher import BulkProcessor, CryptoDataFetcher
from src.loader import CryptoDataLoader
from src.analyzer import QueryExecutor, DatabaseManager
from src.builder import CryptoPricePrediction
from src.logger import setup_logger
from src.settings import DEFAULT_COINS, API_URL_TEMPLATE, DATA, SLEEP_TIME, CONCURRENT_REQUESTS, REQUESTS_PER_MINUTE, API_HEADER, API_KEY, SQL_FILES

# Constants for date formats
DATE_FORMAT = "%Y-%m-%d"

# Logger configuration
logger = setup_logger("main")

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Crypto Data Manager")
    parser.add_argument("--start-date", help="Start date for fetching data (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for fetching data (YYYY-MM-DD)")
    parser.add_argument("--coin-id", help="Comma-separated list of coin identifiers", default=None)
    parser.add_argument("--date", help="Fetch data for a single date (YYYY-MM-DD)")
    parser.add_argument("--store", action="store_true", help="Enable data storage in the database after fetching")
    return parser.parse_args()

def get_coins_from_env() -> list:
    """Get the list of coins from the environment variable, or fallback to default if not set."""
    coins_env = os.getenv("COINS_LIST", "")
    if coins_env:
        return coins_env.split(",")
    else:
        logger.warning("No coins specified in the environment variable, using default.")
        return []

def fetch_data(args, coins_to_fetch: list) -> None:
    """Handle data fetching based on the provided arguments."""
    fetcher = CryptoDataFetcher(
        api_url_template=API_URL_TEMPLATE,
        data_directory=DATA,
        api_header=API_HEADER,
        api_key=API_KEY,
        sleep_time=SLEEP_TIME
    )
    bulk_processor = BulkProcessor(
        fetcher=fetcher,
        concurrent_requests=CONCURRENT_REQUESTS,
        requests_per_minute=REQUESTS_PER_MINUTE
    )

    if args.start_date and args.end_date:
        logger.info(f"Fetching data from {args.start_date} to {args.end_date} for {coins_to_fetch}")
        bulk_processor.bulk_reprocess_data(args.start_date, args.end_date, coins_to_fetch)
    elif args.coin_id and args.date:
        logger.info(f"Fetching data for {args.coin_id} on {args.date}")
        bulk_processor.fetch_single_day_data(args.coin_id, args.date)
    else:
        # Only fetch Bitcoin's data for yesterday
        yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"Fetching yesterday's data ({yesterday}) for bitcoin")
        bulk_processor.fetch_single_day_data("bitcoin", yesterday)


def store_data_if_needed(args) -> None:
    """Process and store data in the database if --store is provided."""
    if not args.store:
        logger.info("Data storage is disabled. Exiting the process.")
        logger.info("The files were successfully saved locally.")
        sys.exit(0)  # Exit the program gracefully
    
    logger.info("Processing and storing fetched data in the database.")
    loader = CryptoDataLoader(sql_files=SQL_FILES, data_dir=DATA, store_data=True)
    loader.process_json_files()

def run_analysis_and_build_models() -> None:
    """Run SQL analysis queries and build models."""
    logger.info("Running SQL analysis queries.")
    db_manager = DatabaseManager()
    query_executor = QueryExecutor(db_manager)
    query_executor.run_sql_queries()

    logger.info("Running model building and prediction process.")
    CryptoPricePrediction.run()

def main() -> None:
    """Main function to orchestrate fetching, loading, analyzing data, and building models."""
    args = parse_args()

    # Determine coins to fetch from the environment variable or fallback to the argument
    coins_to_fetch = get_coins_from_env() if not args.coin_id else args.coin_id.split(",")

    # Fetch data
    fetch_data(args, coins_to_fetch)

    # Process and store data if required
    store_data_if_needed(args)

    # Run SQL analysis and build models
    run_analysis_and_build_models()

if __name__ == "__main__":
    main()
    sys.exit(0)
