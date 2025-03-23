import os
from src.settings import SQL_ANALYSIS_FILE, DAYS_AFTER_DROP, RESULTS_ANALYSIS_FILE, OUTPUTS
from src.logger import setup_logger
from src.database_manager import DatabaseManager
from typing import List

# Logger configuration
logger = setup_logger("query_executor")

def load_queries_from_file(file_path: str) -> List[str]:
    """Loads SQL queries from a file.

    Args:
        file_path (str): Path to the SQL file to load.

    Returns:
        List[str]: A list of SQL queries as strings.
    """
    if not os.path.exists(file_path):
        logger.error(f"SQL file {file_path} not found.")
        return []
    
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            queries = [query.strip() for query in file.read().split(";") if query.strip()]
        return queries
    except Exception as e:
        logger.error(f"Error reading SQL file {file_path}: {e}")
        return []

def write_results_to_file(text: str) -> None:
    """Writes results to a text file.

    Args:
        text (str): The text content to write to the file.
    
    Returns:
        None
    """
    try:
        os.makedirs(OUTPUTS, exist_ok=True)
        with open(RESULTS_ANALYSIS_FILE, "a", encoding="utf-8") as file:
            file.write(text + "\n")
    except Exception as e:
        logger.error(f"Error writing to results file: {e}")

def get_formatted_query(query: str) -> str:
    """Helper function to format SQL queries dynamically.

    Args:
        query (str): The SQL query to format.
    
    Returns:
        str: The formatted SQL query with dynamic parameters.
    """
    return query.format(DAYS_AFTER_DROP=DAYS_AFTER_DROP)

def write_query_results_to_file(idx: int, results: List, result_type: str) -> None:
    """Helper function to write query results to a file in the desired format.

    Args:
        idx (int): The index of the query being executed.
        results (List): The results of the executed query.
        result_type (str): Type or description of the results.

    Returns:
        None
    """
    if idx == 1:
        result_text = f"✅ Average price per coin and month (in USD)"
        write_results_to_file(result_text)  # Save to file
        for coin, year, month, avg_price in results:
            result_line = f"  - Coin: {coin} | Year: {int(year)} | Month: {int(month)} | Average: ${float(avg_price):.2f} USD"
            write_results_to_file(result_line)  # Save to file

    elif idx == 2:
        result_text = f"✅ Average Price recovery after 3 days of consecutive drops on {DAYS_AFTER_DROP} days windows (in USD)"
        write_results_to_file(result_text)  # Save to file
        for coin, avg_price_increase, market_cap_usd in results:
            result_line = f"  - Coin: {coin} | Avg Price Increase: ${avg_price_increase:.2f} | Market Cap: ${market_cap_usd:.2f}"
            write_results_to_file(result_line)  # Save to file

def run_sql_queries() -> None:
    """Executes SQL analysis queries with dynamic parameters.

    Args:
        None

    Returns:
        None
    """
    queries = load_queries_from_file(SQL_ANALYSIS_FILE)

    if not queries:
        logger.warning("No queries found to execute.")
        return

    with DatabaseManager() as db_manager:
        for idx, query in enumerate(queries, start=1):
            formatted_query = get_formatted_query(query)
            logger.info(f"Executing query {idx}: {formatted_query[:50]}...")
            
            try:
                results = db_manager.execute_query(formatted_query)
            except Exception as e:
                logger.error(f"Error executing query {idx}: {e}")
                continue
            
            if not results:
                logger.warning(f"No results for query {idx}.")
                continue

            write_query_results_to_file(idx, results, formatted_query)
