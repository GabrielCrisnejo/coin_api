import sys
import argparse
from datetime import datetime
from src.fetcher import bulk_reprocess_data, fetch_single_day_data
from src.loader import process_json_files
from src.analyzer import run_sql_queries  # Importing the analyzer function
from src.builder import main as builder_main  # Importing the builder's main function
from src.logger import setup_logger
from src.settings import COINS

# Logger configuration
logger = setup_logger("main")

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Crypto Data Manager")
    
    parser.add_argument("--start-date", help="Start date for fetching data (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for fetching data (YYYY-MM-DD)")
    parser.add_argument("--coin-id", help="Comma-separated list of coin identifiers (default: settings.py COINS)", default=None)
    parser.add_argument("--date", help="Fetch data for a single date (YYYY-MM-DD)")
    parser.add_argument("--store", action="store_true", help="Enable data storage in the database after fetching")

    return parser.parse_args()

def main():
    """Main function to orchestrate fetching, loading, analyzing data, and building models."""
    args = parse_args()
    
    # Determine coins to fetch
    coins_to_fetch = COINS if not args.coin_id else args.coin_id.split(",")

    if args.start_date and args.end_date:
        # Bulk fetch data
        logger.info(f"Fetching data from {args.start_date} to {args.end_date} for {coins_to_fetch}")
        bulk_reprocess_data(args.start_date, args.end_date, coins_to_fetch)
    elif args.coin_id and args.date:
        # Fetch single day data
        logger.info(f"Fetching data for {args.coin_id} on {args.date}")
        fetch_single_day_data(args.coin_id, args.date)
    else:
        # Default: Fetch today's data
        today = datetime.today().strftime("%Y-%m-%d")
        logger.info(f"Fetching today's data ({today}) for {coins_to_fetch}")
        bulk_reprocess_data(today, today, coins_to_fetch)

    # Process and store data in the database if --store is provided
    if args.store:
        logger.info("Processing and storing fetched data in the database.")
        process_json_files(store_data=True)
    else:
        logger.info("Skipping database storage as --store flag is not provided.")

    # Run SQL analysis queries automatically
    logger.info("Running SQL analysis queries.")
    run_sql_queries()

    # Call the builder module to build models and make predictions
    logger.info("Running model building and prediction process.")
    builder_main()  # Call the main function of builder.py for building models and predictions

if __name__ == "__main__":
    main()

    sys.exit(0) 
