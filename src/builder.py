import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
import holidays
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
from settings import DB_URL
from logger import setup_logger

# Logger configuration
logger = setup_logger("crypto_analysis")

# Database connection setup
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
        """Executes an SQL query and returns the results."""
        try:
            result = self.session.execute(text(query))
            return result.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return None

def fetch_crypto_data():
    """Fetches cryptocurrency data from the database for the last 30 days."""
    today = datetime.today()
    thirty_days_ago = today - timedelta(days=30)
    query = f"""
    SELECT coin_id, date, price_usd, volume_usd
    FROM raw_crypto_data
    WHERE date >= '{thirty_days_ago.strftime('%Y-%m-%d')}'
    ORDER BY coin_id, date;
    """
    logger.info("Fetching cryptocurrency data for the last 30 days...")
    with DatabaseManager() as db_manager:
        return db_manager.execute_query(query)

def preprocess_data(data):
    """Processes the raw data into a DataFrame with additional features."""
    df = pd.DataFrame(data, columns=['coin_id', 'date', 'price_usd', 'volume_usd'])
    df['date'] = pd.to_datetime(df['date'])
    df = df.groupby('coin_id', group_keys=False).apply(compute_features)
    df = df.dropna(subset=[f'price_lag_{i}' for i in range(1, 8)])
    df = df.dropna(subset=['price_target'])
    logger.info("Data preprocessing completed.")
    return df

def compute_features(group):
    """Computes financial indicators and lag features for model training."""
    logger.info(f"Computing features for coin: {group['coin_id'].iloc[0]}")
    group['price_diff'] = group['price_usd'].pct_change()
    group['risk'] = group['price_diff'].apply(lambda x: 'High Risk' if x <= -0.5 else 'Medium Risk' if x <= -0.2 else 'Low Risk')
    group['price_diff_7_days'] = group['price_usd'].diff(7)
    group['variance_7_days'] = group['price_usd'].rolling(window=7).var()
    for i in range(1, 8):
        group[f'price_lag_{i}'] = group['price_usd'].shift(i)
    group['price_target'] = group['price_usd'].shift(-1)
    group['day_of_week'] = group['date'].dt.dayofweek
    group['is_weekend'] = group['day_of_week'].isin([5, 6]).astype(int)
    group['week_of_year'] = group['date'].dt.isocalendar().week
    group['month'] = group['date'].dt.month
    us_holidays = holidays.US()
    china_holidays = holidays.China()
    group['is_us_holiday'] = group['date'].isin(us_holidays).astype(int)
    group['is_china_holiday'] = group['date'].isin(china_holidays).astype(int)
    return group

def train_and_evaluate_model(df, model_type="linear"):
    """Trains and evaluates a regression model (Linear or XGBoost) for each cryptocurrency."""
    for coin_id, group in df.groupby("coin_id"):
        logger.info(f"🚀 Training {model_type} model for {coin_id}...")
        features = [f'price_lag_{i}' for i in range(1, 8)]
        X = group[features]
        y = group['price_target']
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
        if model_type == "linear":
            model = LinearRegression()
        elif model_type == "xgboost":
            model = xgb.XGBRegressor(objective='reg:squarederror', random_state=42)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        logger.info(f"🎯 {model_type.capitalize()} Model MAE for {coin_id}: {mae:.4f}")

def plot_price_trends(df):
    """Generates and saves price trend plots."""
    os.makedirs("plots", exist_ok=True)
    for coin_id, group in df.groupby("coin_id"):
        plt.figure(figsize=(10, 5))
        plt.plot(group["date"], group["price_usd"], marker="o", linestyle="-", label=f"{coin_id} Price")
        plt.xlabel("Date")
        plt.ylabel("Price (USD)")
        plt.title(f"Price Trend for {coin_id}")
        plt.legend()
        plt.savefig(f"plots/{coin_id}_price_trend.png")
        plt.close()

def main():
    logger.info("Starting cryptocurrency price prediction process...")
    data = fetch_crypto_data()
    if not data:
        logger.error("No data retrieved from the database.")
        return
    df = preprocess_data(data)
    plot_price_trends(df)
    train_and_evaluate_model(df, "linear")
    train_and_evaluate_model(df, "xgboost")
    logger.info("✅ Prediction process completed successfully.")

if __name__ == "__main__":
    main()
