import os

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DATA_DIR_BULK = os.getenv("DATA_DIR_BULK", os.path.join(base_dir, "data", "downloads", "bulk"))
DATA_DIR_SINGLE = os.getenv("DATA_DIR_SINGLE", os.path.join(base_dir, "data", "downloads", "single"))
CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", 20))
REQUESTS_PER_MINUTE = int(os.getenv("REQUESTS_PER_MINUTE", 30))
SLEEP_TIME = int(os.getenv("SLEEP_TIME", 10))
API_URL_TEMPLATE = os.getenv("API_URL", "https://api.coingecko.com/api/v3/coins/{coin_id}/history")
LOG_FILE = os.getenv("LOG_FILE", os.path.join(base_dir, "logs", "logger.log"))

# Default for bulk-processing
COINS = os.getenv("COINS", "bitcoin,ethereum,cardano").split(",")

# Default CRON format for 3 AM every day
SCHEDULE = os.getenv("SCHEDULE", "0 3 * * *") # format M H every day
COINS_CRON = os.getenv("COINS_CRON", "bitcoin,ethereum,cardano").split(",")

DB_NAME = os.getenv("DB_NAME", "crypto_db")
DB_USER = os.getenv("DB_USER", "gcrisnejo")
DB_PASSWORD = os.getenv("DB_PASSWORD", "gcrisnejo")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_URL = os.getenv("DB_URL", f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Paths for SQL schema files
SQL_FILES = [
    os.path.join(base_dir, os.getenv("SQL_FILE_RAW", os.path.join("sql", "01_create_raw_data_table.sql"))),
    os.path.join(base_dir, os.getenv("SQL_FILE_AGG", os.path.join("sql", "02_create_aggregated_data_table.sql"))),
]

# Directory where JSON files are stored
JSON_FILES_PATH = os.getenv("JSON_FILES_PATH", os.path.join(base_dir, "data", "testing"))
