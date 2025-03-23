import os
from pathlib import Path

# Define base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Directories
DATA = os.getenv("DATA", str(BASE_DIR / "data"))
OUTPUTS = os.getenv("OUTPUTS", str(BASE_DIR / "outputs"))
PLOTS_STORE = os.getenv("PLOTS_STORE", str(BASE_DIR / "plots"))
LOG_FILE = os.getenv("LOG_FILE", str(BASE_DIR / "logs" / "logger.log"))
RESULTS_ANALYSIS_FILE = os.getenv("RESULTS_ANALYSIS_FILE", str(BASE_DIR / "outputs" / "analysis.txt"))
RESULTS_MODELS_FILE = os.getenv("RESULTS_MODELS_FILE", str(BASE_DIR / "outputs" / "model_results.json"))

# SQL files
SQL_DIR = BASE_DIR / "sql"
SQL_ANALYSIS_FILE = os.getenv("SQL_ANALYSIS_FILE", str(SQL_DIR / "analysis_queries.sql"))
SQL_FILES = [
    str(SQL_DIR / os.getenv("SQL_FILE_RAW", "01_create_raw_data_table.sql")),
    str(SQL_DIR / os.getenv("SQL_FILE_AGG", "02_create_aggregated_data_table.sql")),
]

# API settings
API_HEADER = os.getenv("API_HEADER","x-cg-demo-api-key")
API_KEY = os.getenv("API_KEY","CG-aKrr3PfqgQRBCtgLspoRKC5P")
CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", 20))
REQUESTS_PER_MINUTE = int(os.getenv("REQUESTS_PER_MINUTE", 30))
SLEEP_TIME = int(os.getenv("SLEEP_TIME", 10))
API_URL_TEMPLATE = os.getenv("API_URL", "https://api.coingecko.com/api/v3/coins/{coin_id}/history")

# Default Cryptocurrencies to track
DEFAULT_COINS = os.getenv("DEFAULT_COINS", "bitcoin")

# CRON schedule
SCHEDULE = os.getenv("SCHEDULE", "0 3 * * *")  # Default: 3 AM every day
COINS_CRON = os.getenv("COINS_CRON", "bitcoin,ethereum,cardano").split(",")

# Database configuration
DB_NAME = os.getenv("DB_NAME", "crypto_db")
DB_USER = os.getenv("DB_USER", "gcrisnejo")
DB_PASSWORD = os.getenv("DB_PASSWORD", "gcrisnejo")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_URL = os.getenv("DB_URL", f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Days after price drop for analysis
DAYS_AFTER_DROP = int(os.getenv("DAYS_AFTER_DROP", 2))
TRAINING_DAYS = int(os.getenv("TRAINING_DAYS", 30))
