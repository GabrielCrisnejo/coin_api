import os
import json
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from settings import *
from logger import *

# Logger configuration
logger = setup_logger("loader")

# Create SQLAlchemy engine
engine = create_engine(DB_URL, echo=False, pool_pre_ping=True)
Session = sessionmaker(bind=engine)

class DatabaseManager:
    def __enter__(self):
        self.session = Session()
        logger.info("Connected to the database successfully.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        logger.info("Database connection closed.")

    def execute_sql_file(self, file_path):
        if not os.path.exists(file_path):
            logger.warning(f"SQL file {file_path} not found.")
            return

        try:
            with open(file_path, "r") as file:
                sql_commands = file.read()
                self.session.execute(text(sql_commands))
                self.session.commit()
            logger.info(f"Executed {file_path} successfully.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error executing {file_path}: {e}")

    def insert_raw_data(self, data, coin_id, date):
        try:
            query = text("""
                INSERT INTO raw_crypto_data (coin_id, date, price_usd, volume_usd, raw_json)
                VALUES (:coin_id, :date, :price_usd, :volume_usd, :raw_json)
                ON CONFLICT (coin_id, date) DO NOTHING
            """)
            self.session.execute(query, {
                "coin_id": coin_id,
                "date": date,
                "price_usd": data['market_data']['current_price']['usd'],
                "volume_usd": data['market_data']['total_volume']['usd'],
                "raw_json": json.dumps(data),
            })
            self.session.commit()
            logger.info(f"Inserted raw data for {coin_id} on {date}.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error inserting raw data: {e}")

    def insert_aggregated_data(self, data, coin_id, year, month, day):
        try:
            new_price = data['market_data']['current_price']['usd']
            query = text("""
                SELECT max_price, min_price FROM aggregated_crypto_data
                WHERE coin_id = :coin_id AND year = :year AND month = :month
            """)
            result = self.session.execute(query, {"coin_id": coin_id, "year": year, "month": month}).fetchone()

            if result:
                max_price, min_price = result
                max_price = max(max_price, new_price)
                min_price = min(min_price, new_price)
                update_query = text("""
                    UPDATE aggregated_crypto_data
                    SET max_price = :max_price, min_price = :min_price
                    WHERE coin_id = :coin_id AND year = :year AND month = :month
                """)
                self.session.execute(update_query, {
                    "max_price": max_price,
                    "min_price": min_price,
                    "coin_id": coin_id,
                    "year": year,
                    "month": month
                })
                logger.info(f"Updated aggregated data for {coin_id} on {year}-{month}-{day}.")
            else:
                insert_query = text("""
                    INSERT INTO aggregated_crypto_data (coin_id, year, month, max_price, min_price)
                    VALUES (:coin_id, :year, :month, :max_price, :min_price)
                """)
                self.session.execute(insert_query, {
                    "coin_id": coin_id,
                    "year": year,
                    "month": month,
                    "max_price": new_price,
                    "min_price": new_price
                })
                logger.info(f"Inserted aggregated data for {coin_id} on {year}-{month}-{day}.")
            
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error inserting or updating aggregated data: {e}")


def get_json_files(directory):
    """Returns a list of JSON files in the given directory."""
    try:
        return [f for f in os.listdir(directory) if f.endswith('.json')]
    except Exception as e:
        logger.error(f"Error accessing JSON files in {directory}: {e}")
        return []


def process_json_files(store_data):
    with DatabaseManager() as db_manager:
        # Execute SQL schema files
        for sql_file in SQL_FILES:
            db_manager.execute_sql_file(sql_file)

        # Process JSON files
        for json_file in get_json_files(JSON_FILES_PATH):
            file_path = os.path.join(JSON_FILES_PATH, json_file)
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    coin_id, date_str = json_file.replace('.json', '').split('_')
                    year, month, day = date_str.split('-')

                    if store_data:
                        db_manager.insert_raw_data(data, coin_id, f'{year}-{month}-{day}')
                        db_manager.insert_aggregated_data(data, coin_id, year, month, day)
            except Exception as e:
                logger.error(f"Error processing {json_file}: {e}")

        logger.info("Data processing completed.")


def parse_args():
    parser = argparse.ArgumentParser(description="Load cryptocurrency data into PostgreSQL.")
    parser.add_argument('--store', action='store_true', help="Enable data storage in the database")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    process_json_files(args.store)
