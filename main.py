import os
import sys
import argparse
from datetime import datetime, timedelta
from src.fetcher import bulk_reprocess_data, fetch_single_day_data
from src.loader import process_json_files
from src.analyzer import run_sql_queries
from src.builder import builder_main
from src.logger import setup_logger
from src.settings import DEFAULT_COINS

# Constants for date formats
DATE_FORMAT = "%Y-%m-%d"

# Logger configuration
logger = setup_logger("main")

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Crypto Data Manager")
    parser.add_argument("--start-date", help="Start date for fetching data (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for fetching data (YYYY-MM-DD)")
    parser.add_argument("--coin-id", help="Comma-separated list of coin identifiers", default=None)
    parser.add_argument("--date", help="Fetch data for a single date (YYYY-MM-DD)")
    parser.add_argument("--store", action="store_true", help="Enable data storage in the database after fetching")
    return parser.parse_args()

def get_coins_from_env() -> list:
    """Get the list of coins from the environment variable, or fallback to default if not set.

    Returns:
        list: A list of coin identifiers, either from the environment variable or default.
    """
    coins_env = os.getenv("COINS_LIST", "")  # Default to empty string if not set
    if coins_env:
        return coins_env.split(",")  # Split by commas into a list
    else:
        logger.warning("No coins specified in the environment variable, using default.")
        return []  # Or return a default set of coins if required

def fetch_data(args, coins_to_fetch: list) -> None:
    """Handle data fetching based on the provided arguments.

    Args:
        args (argparse.Namespace): The parsed command-line arguments.
        coins_to_fetch (list): A list of coin identifiers to fetch data for.

    Returns:
        None
    """
    if args.start_date and args.end_date:
        logger.info(f"Fetching data from {args.start_date} to {args.end_date} for {coins_to_fetch}")
        bulk_reprocess_data(args.start_date, args.end_date, coins_to_fetch)
    elif args.coin_id and args.date:
        logger.info(f"Fetching data for {args.coin_id} on {args.date}")
        fetch_single_day_data(args.coin_id, args.date)
    else:
        yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"Fetching yesterday's data ({yesterday}) for {DEFAULT_COINS}")
        fetch_single_day_data(DEFAULT_COINS, yesterday)

def store_data_if_needed(args) -> None:
    """Process and store data in the database if --store is provided.

    Args:
        args (argparse.Namespace): The parsed command-line arguments.

    Returns:
        None
    """
    if args.store:
        logger.info("Processing and storing fetched data in the database.")
        process_json_files(store_data=True)
    else:
        logger.info("Skipping database storage as --store flag is not provided.")

def run_analysis_and_build_models() -> None:
    """Run SQL analysis queries and build models.

    Returns:
        None
    """
    logger.info("Running SQL analysis queries.")
    run_sql_queries()
    logger.info("Running model building and prediction process.")
    builder_main()

def main() -> None:
    """Main function to orchestrate fetching, loading, analyzing data, and building models.

    Returns:
        None
    """
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
