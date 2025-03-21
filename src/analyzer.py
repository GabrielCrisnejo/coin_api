import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.settings import * # Asegúrate de tener tu archivo settings.py configurado
from src.logger import * # Asegúrate de tener tu archivo logger.py configurado

# Configuración del logger
logger = setup_logger("query_executor")

# Configuración de la conexión a la base de datos
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

    def execute_query(self, query):
        """Ejecuta una consulta SQL y devuelve los resultados."""
        try:
            result = self.session.execute(text(query))
            return result.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return None

def load_queries_from_file(file_path):
    """Carga las consultas SQL desde un archivo."""
    if not os.path.exists(file_path):
        logger.error(f"SQL file {file_path} not found.")
        return []

    with open(file_path, "r") as file:
        queries = [query.strip() for query in file.read().split(";") if query.strip()]
    return queries

def run_sql_queries():
    """Ejecuta las consultas SQL de análisis, permitiendo parámetros dinámicos."""
    queries = load_queries_from_file(SQL_ANALYSIS_FILE)

    if not queries:
        logger.warning("No queries found to execute.")
        return

    with DatabaseManager() as db_manager:
        for idx, query in enumerate(queries, start=1):
            # Reemplazar el marcador con el valor real desde settings.py
            formatted_query = query.format(DAYS_AFTER_DROP=DAYS_AFTER_DROP)

            logger.info(f"Executing query {idx}: {formatted_query[:50]}...")
            results = db_manager.execute_query(formatted_query)

            if not results:
                logger.warning(f"No results for query {idx}.")
                continue

            # Improved result formatting
            if idx == 1:
                logger.info("Average price per coin and month (in USD):")
                for coin, year, month, avg_price in results:
                    logger.info(f"  - coin: {coin} | year: {int(year)} | month: {int(month)} | average: ${float(avg_price):.2f} USD")

            elif idx == 2:
                logger.info(f"Average Price recovery after consecutive drops of {DAYS_AFTER_DROP} days (in USD):")
                for coin, avg_price_increase, market_cap_usd in results:
                    logger.info(
                        f"Coin: {coin} | Avg Price Increase: ${avg_price_increase:.2f} | Market Cap: ${market_cap_usd:.2f}"
                    )

if __name__ == "__main__":
    run_sql_queries()