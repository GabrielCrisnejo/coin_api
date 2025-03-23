import os
from typing import List
from src.settings import SQL_ANALYSIS_FILE, DAYS_AFTER_DROP, RESULTS_ANALYSIS_FILE, OUTPUTS
from src.logger import setup_logger
from src.database_manager import DatabaseManager

class QueryExecutor:
    def __init__(self, db_manager: DatabaseManager):
        """Initializes the QueryExecutor with a database manager."""
        self.logger = setup_logger("query_executor")
        self.db_manager = db_manager

    def load_queries_from_file(self, file_path: str) -> List[str]:
        """Loads SQL queries from a file."""
        if not os.path.exists(file_path):
            self.logger.error(f"SQL file {file_path} not found.")
            return []
        
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                queries = [query.strip() for query in file.read().split(";") if query.strip()]
            return queries
        except Exception as e:
            self.logger.error(f"Error reading SQL file {file_path}: {e}")
            return []

    def write_results_to_file(self, text: str) -> None:
        """Writes results to a text file."""
        try:
            os.makedirs(OUTPUTS, exist_ok=True)
            with open(RESULTS_ANALYSIS_FILE, "a", encoding="utf-8") as file:
                file.write(text + "\n")
        except Exception as e:
            self.logger.error(f"Error writing to results file: {e}")

    def get_formatted_query(self, query: str) -> str:
        """Formats SQL queries dynamically."""
        return query.format(DAYS_AFTER_DROP=DAYS_AFTER_DROP)

    def write_query_results_to_file(self, idx: int, results: List, result_type: str) -> None:
        """Writes query results to a file in a formatted manner."""
        if idx == 1:
            result_text = "✅ Average price per coin and month (in USD)"
            self.write_results_to_file(result_text)
            for coin, year, month, avg_price in results:
                result_line = f"  - Coin: {coin} | Year: {int(year)} | Month: {int(month)} | Average: ${float(avg_price):.2f} USD"
                self.write_results_to_file(result_line)

        elif idx == 2:
            result_text = f"✅ Average Price recovery after 3 days of consecutive drops on {DAYS_AFTER_DROP} days windows (in USD)"
            self.write_results_to_file(result_text)
            for coin, avg_price_increase, market_cap_usd in results:
                result_line = f"  - Coin: {coin} | Avg Price Increase: ${avg_price_increase:.2f} | Market Cap: ${market_cap_usd:.2f}"
                self.write_results_to_file(result_line)

    def run_sql_queries(self) -> None:
        """Executes SQL analysis queries with dynamic parameters."""
        queries = self.load_queries_from_file(SQL_ANALYSIS_FILE)

        if not queries:
            self.logger.warning("No queries found to execute.")
            return

        with self.db_manager as db_manager:
            for idx, query in enumerate(queries, start=1):
                formatted_query = self.get_formatted_query(query)
                self.logger.info(f"Executing query {idx}: {formatted_query[:50]}...")

                try:
                    results = db_manager.execute_query(formatted_query)
                except Exception as e:
                    self.logger.error(f"Error executing query {idx}: {e}")
                    continue

                if not results:
                    self.logger.warning(f"No results for query {idx}.")
                    continue

                self.write_query_results_to_file(idx, results, formatted_query)