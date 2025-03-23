import os
import json
import argparse
from src.settings import SQL_FILES, DATA
from src.logger import setup_logger
from src.database_manager import DatabaseManager
from typing import List
import sys

# Logger configuration
logger = setup_logger("loader")

class CryptoDataLoader:
    """Class to load cryptocurrency data from JSON files and store them in a database."""
    
    def __init__(self, sql_files: List[str], data_dir: str, store_data: bool = False):
        self.sql_files = sql_files
        self.data_dir = data_dir
        self.store_data = store_data

    def get_json_files(self) -> List[str]:
        """Returns a list of JSON files in the given directory.

        Returns:
            List[str]: A list of filenames (with `.json` extension) found in the directory.
        """
        try:
            return [f for f in os.listdir(self.data_dir) if f.endswith('.json')]
        except OSError as e:
            logger.error(f"Error accessing JSON files in {self.data_dir}: {e}")
            return []

    def process_json_files(self) -> None:
        """Processes JSON files and loads data into the database.

        Returns:
            None
        """
        with DatabaseManager() as db_manager:
            # Execute SQL schema files
            for sql_file in self.sql_files:
                db_manager.execute_sql_file(sql_file)

            # Process JSON files
            for json_file in self.get_json_files():
                file_path = os.path.join(self.data_dir, json_file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        coin_id, date_str = json_file.replace('.json', '').split('_')
                        year, month, day = map(int, date_str.split('-'))
                        
                        if self.store_data:
                            db_manager.insert_raw_data(data, coin_id, f'{year}-{month}-{day}')
                            db_manager.insert_aggregated_data(data, coin_id, year, month, day)

                    logger.info(f"✅ Successfully processed {json_file}")
                except (json.JSONDecodeError, ValueError) as e:
                    logger.error(f"❌ Error parsing {json_file}: {e}")
                except Exception as e:
                    logger.error(f"🚨 Unexpected error processing {json_file}: {e}")

        logger.info("✅ Data processing completed.")

    @staticmethod
    def parse_args() -> argparse.Namespace:
        """Parses command-line arguments.

        Returns:
            argparse.Namespace: The parsed command-line arguments.
        """
        parser = argparse.ArgumentParser(description="Load cryptocurrency data into PostgreSQL.")
        parser.add_argument('--store', action='store_true', help="Enable data storage in the database")
        return parser.parse_args()
