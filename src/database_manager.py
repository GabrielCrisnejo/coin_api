import os
import json
import threading
from typing import Any, Optional, List, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession
from src.logger import setup_logger
from src.settings import DB_URL

# Logger configuration
logger = setup_logger("crypto_analysis")

# Database connection setup
engine = create_engine(DB_URL, echo=False, pool_pre_ping=True)
Session = sessionmaker(bind=engine)

class DatabaseManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """Ensures a single instance of DatabaseManager (Singleton Pattern)."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:  # Double-check locking
                    cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def __enter__(self) -> "DatabaseManager":
        """Opens a new database session.

        Args:
            None

        Returns:
            DatabaseManager: The instance of the DatabaseManager class.
        """
        self.session: SQLAlchemySession = Session()
        if not hasattr(self, "_logged"):
            logger.info("Connected to the database successfully.")
            self._logged = True  # Ensure logging happens only once
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        """Closes the database session.

        Args:
            exc_type (Optional[type]): The exception type, if any, raised during the context.
            exc_val (Optional[Exception]): The exception value, if any.
            exc_tb (Optional[Any]): The traceback, if any.

        Returns:
            None
        """
        self.session.close()
        logger.info("Database connection closed.")

    def execute_query(self, query: str) -> Optional[List[Tuple]]:
        """Executes a SQL query and returns the results.

        Args:
            query (str): The SQL query string to be executed.

        Returns:
            Optional[List[Tuple]]: A list of tuples containing the query results, or None if an error occurred.
        """
        try:
            result = self.session.execute(text(query))
            return result.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return None

    def execute_sql_file(self, file_path: str) -> None:
        """Executes SQL statements from a file.

        Args:
            file_path (str): The path to the SQL file containing the queries to be executed.

        Returns:
            None
        """
        if not os.path.exists(file_path):
            logger.warning(f"SQL file {file_path} not found.")
            return

        try:
            with open(file_path, "r", encoding="utf-8") as file:
                sql_commands = file.read()
                self.session.execute(text(sql_commands))
                self.session.commit()
            logger.info(f"Executed {file_path} successfully.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error executing {file_path}: {e}")

    def insert_raw_data(self, data: dict, coin_id: str, date: str) -> None:
        """Inserts raw cryptocurrency data into the database.

        Args:
            data (dict): A dictionary containing raw cryptocurrency data.
            coin_id (str): The coin's unique identifier.
            date (str): The date of the data.

        Returns:
            None
        """
        try:
            query = text("""
                INSERT INTO raw_crypto_data (coin_id, date, price_usd, volume_usd, raw_json)
                VALUES (:coin_id, :date, :price_usd, :volume_usd, :raw_json)
                ON CONFLICT (coin_id, date) DO NOTHING
            """
            )
            self.session.execute(query, {
                "coin_id": coin_id,
                "date": date,
                "price_usd": data['market_data']['current_price']['usd'],
                "volume_usd": data['market_data']['total_volume']['usd'],
                "raw_json": json.dumps(data),
            })
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error inserting raw data: {e}")

    def insert_aggregated_data(self, data: dict, coin_id: str, year: int, month: int, day: int) -> None:
        """Inserts or updates aggregated cryptocurrency data in the database.

        Args:
            data (dict): A dictionary containing aggregated cryptocurrency data.
            coin_id (str): The coin's unique identifier.
            year (int): The year of the aggregated data.
            month (int): The month of the aggregated data.
            day (int): The day of the aggregated data.

        Returns:
            None
        """
        try:
            new_price = data['market_data']['current_price']['usd']
            query = text("""
                SELECT max_price, min_price FROM aggregated_crypto_data
                WHERE coin_id = :coin_id AND year = :year AND month = :month
            """
            )
            result = self.session.execute(query, {"coin_id": coin_id, "year": year, "month": month}).fetchone()

            if result:
                max_price, min_price = result
                max_price = max(max_price, new_price)
                min_price = min(min_price, new_price)
                update_query = text("""
                    UPDATE aggregated_crypto_data
                    SET max_price = :max_price, min_price = :min_price
                    WHERE coin_id = :coin_id AND year = :year AND month = :month
                """
                )
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
                """
                )
                self.session.execute(insert_query, {
                    "coin_id": coin_id,
                    "year": year,
                    "month": month,
                    "max_price": new_price,
                    "min_price": new_price
                })
                logger.info(f"Updated aggregated data for {coin_id} on {year}-{month}-{day}.")
            
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error inserting or updating aggregated data: {e}")
