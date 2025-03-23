import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import holidays
import json
import os
from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
from typing import List, Tuple, Optional
from src.settings import PLOTS_STORE, RESULTS_MODELS_FILE, TRAINING_DAYS
from src.logger import setup_logger
from src.database_manager import DatabaseManager

# Logger configuration
logger = setup_logger("crypto_analysis")


class CryptoDataFetcherDatabase:
    """Handles the fetching of cryptocurrency data from the database."""

    @staticmethod
    def fetch(last_days: bool = False) -> Optional[List[Tuple[str, str, float, float]]]:
        """Fetches cryptocurrency data from the database.
        
        Args:
            last_days (bool): If True, fetches data for the last TRAINING_DAYS days. Default is False, which fetches all available data.
        
        Returns:
            Optional[List[Tuple[str, str, float, float]]]: A list of tuples containing coin ID, date, price (USD), and volume (USD). Returns None if there was an error.
        """
        try:
            query = """
            SELECT coin_id, date, price_usd, volume_usd
            FROM raw_crypto_data
            """
            
            if last_days:
                today = datetime.today()
                start_date = today - timedelta(days=TRAINING_DAYS)
                query += f" WHERE date >= '{start_date.strftime('%Y-%m-%d')}'"
                logger.info(f"Fetching cryptocurrency data for the last {TRAINING_DAYS} days...")
            else:
                logger.info("Fetching all cryptocurrency data...")
    
            query += " ORDER BY coin_id, date;"
    
            with DatabaseManager() as db_manager:
                return db_manager.execute_query(query)
        except Exception as e:
            logger.error(f"Error fetching cryptocurrency data: {e}")
            return None


class CryptoDataPreprocessor:
    """Processes raw cryptocurrency data into a structured DataFrame with additional features."""
    
    @staticmethod
    def preprocess(data: List[Tuple[str, str, float, float]]) -> Optional[pd.DataFrame]:
        """Processes the raw data into a DataFrame with additional features.
        
        Args:
            data (List[Tuple[str, str, float, float]]): Raw cryptocurrency data, each entry containing coin ID, date, price (USD), and volume (USD).
        
        Returns:
            Optional[pd.DataFrame]: A processed DataFrame with additional features. Returns None if there was an error.
        """
        try:
            df = pd.DataFrame(data, columns=['coin_id', 'date', 'price_usd', 'volume_usd'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.groupby('coin_id', group_keys=False).apply(CryptoDataPreprocessor.compute_features)
            df.dropna(subset=[f'price_lag_{i}' for i in range(1, 8)], inplace=True)
            df.dropna(subset=['price_target'], inplace=True)
            logger.info("Data preprocessing completed.")
            return df
        except Exception as e:
            logger.error(f"Error preprocessing data: {e}")
            return None

    @staticmethod
    def compute_features(group: pd.DataFrame) -> pd.DataFrame:
        """Computes financial indicators and lag features for model training.

        Args:
            group (pd.DataFrame): Data for a single cryptocurrency.

        Returns:
            pd.DataFrame: The original DataFrame with additional features like price differences, lags, risk labels, etc.
        """
        try:
            logger.info(f"Computing features for coin: {group['coin_id'].iloc[0]}")
            
            group = group.sort_values(by='date').reset_index(drop=True)
            
            # Daily percentage price change
            group['price_diff'] = group['price_usd'].pct_change()
            group['drop_50'] = group['price_diff'] <= -0.5
            group['drop_20'] = group['price_diff'] <= -0.2
            group['consec_drop_50'] = group['drop_50'] & group['drop_50'].shift(1)
            group['consec_drop_20'] = group['drop_20'] & group['drop_20'].shift(1)
            
            group['year_month'] = group['date'].dt.to_period('M')
            risk_labels = {}
            for period, sub_df in group.groupby('year_month'):
                if sub_df['consec_drop_50'].any():
                    risk = 'High Risk'
                elif sub_df['consec_drop_20'].any():
                    risk = 'Medium Risk'
                else:
                    risk = 'Low Risk'
                risk_labels[period] = risk
            group['risk'] = group['year_month'].map(risk_labels)

            group['price_diff_7_days'] = group['price_usd'] - group['price_usd'].shift(7)
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

            group['volume_var_7_days'] = group['volume_usd'].rolling(window=7).var()
            group['volume_avg_7_days'] = group['volume_usd'].rolling(window=7).mean()

            group.drop(columns=['drop_50', 'drop_20', 'consec_drop_50', 'consec_drop_20', 'year_month'], inplace=True)

            return group
        except Exception as e:
            logger.error(f"Error computing features for {group['coin_id'].iloc[0]}: {e}")
            return group


class ModelResultsWriter:
    """Handles writing model results to a JSON file."""

    @staticmethod
    def write(results: list, filename: str = None) -> None:
        """Writes the model results to a JSON file.
        
        Args:
            results (list): A list of dictionaries with model results.
            filename (str): Name of the output JSON file. If None, uses the value from settings.py.
        
        Returns:
            None
        """
        try:
            if filename is None:
                filename = RESULTS_MODELS_FILE
            
            output_dir = os.path.dirname(filename)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            def convert_floats(obj):
                if isinstance(obj, np.float32):
                    return float(obj)
                elif isinstance(obj, dict):
                    return {key: convert_floats(value) for key, value in obj.items()}
                elif isinstance(obj, list):
                    return [convert_floats(item) for item in obj]
                else:
                    return obj

            results = convert_floats(results)

            if os.path.exists(filename):
                with open(filename, 'r') as file:
                    try:
                        existing_data = json.load(file)
                    except json.JSONDecodeError:
                        logger.warning(f"⚠️ JSON decode error. Starting with an empty list.")
                        existing_data = []
            else:
                existing_data = []

            combined_results = existing_data + results

            unique_results = []
            seen = set()
            for item in combined_results:
                key = (item["coin_id"], item["model_type"])
                if key not in seen:
                    seen.add(key)
                    unique_results.append(item)

            with open(filename, 'w') as file:
                json.dump(unique_results, file, indent=4)

            logger.info(f"✅ Results successfully saved to {filename}")

        except Exception as e:
            logger.error(f"❌ Error writing to JSON file {filename}: {e}")


class ModelTrainer:
    """Handles training and evaluating models (LinearRegression or XGBRegressor)."""

    @staticmethod
    def train_and_evaluate(df: pd.DataFrame, model_type: str = "linear") -> None:
        """Trains and evaluates a regression model (Linear or XGBoost) for each cryptocurrency.
        
        Args:
            df (pd.DataFrame): The processed DataFrame with features and target values.
            model_type (str): The type of model to use ("linear" for LinearRegression or "xgboost" for XGBRegressor).
        
        Returns:
            None
        """
        results = []
        prediction_date = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')

        try:
            for coin_id, group in df.groupby("coin_id"):
                logger.info(f"🚀 Training {model_type} model for {coin_id}...")

                features = [f'price_lag_{i}' for i in range(1, 8)]
                X = group[features]
                y = group['price_target']

                scaler = StandardScaler()
                scaler.fit(X)
                X_scaled = scaler.transform(X)

                X_train, X_test, y_train, y_test = train_test_split(
                    X_scaled, y, test_size=0.2, random_state=42
                )

                if model_type == "linear":
                    model = LinearRegression()
                elif model_type == "xgboost":
                    model = xgb.XGBRegressor()
                else:
                    logger.warning("❌ Invalid model type!")
                    continue

                model.fit(X_train, y_train)

                y_pred = model.predict(X_test)
                mae = mean_absolute_error(y_test, y_pred)
                results.append({
                    "coin_id": coin_id,
                    "model_type": model_type,
                    "MAE": mae,
                    "prediction_date": prediction_date,
                    "predicted_price": y_pred[-1],
                    "currency": "USD",
                })

            ModelResultsWriter.write(results)

        except Exception as e:
            logger.error(f"Error in training and evaluating model: {e}")

    @staticmethod
    def generate_plots(group: pd.DataFrame, model, scaler, coin_id: str) -> None:
        """Generates prediction plots for the cryptocurrency with points connected by lines and grid."""
        try:
            logger.info(f"Generating raw data plot for {coin_id}...")

            # Use raw price_usd for plotting, not the processed data
            plt.figure(figsize=(10, 6))
            plt.plot(group['date'], group['price_usd'], label=f'{coin_id} Raw Price', marker='o', linestyle='-', color='b')  # Raw price
            plt.title(f'Raw Price Trend for {coin_id}')
            plt.xlabel('Date')
            plt.ylabel('Price (USD)')
            plt.legend()
            plt.xticks(rotation=45)
            plt.tight_layout()

            # Add grid for better readability
            plt.grid(True)

            # Ensure the directory exists before saving the plot
            os.makedirs(PLOTS_STORE, exist_ok=True)

            plot_filename = os.path.join(PLOTS_STORE, f"{coin_id}_raw_price_trend.png")
            plt.savefig(plot_filename)
            logger.info(f"✅ Raw price plot saved to {plot_filename}")
            plt.close()  # Close the plot after saving to avoid memory overload
        except Exception as e:
            logger.error(f"Error generating raw price plot for {coin_id}: {e}")


class CryptoPricePrediction:
    """Main class to orchestrate the cryptocurrency price prediction process."""

    @staticmethod
    def run() -> None:
        """Main function that initiates the cryptocurrency price prediction process.
        
        Args:
            None
        
        Returns:
            None
        """
        try:
            logger.info("Starting cryptocurrency price prediction process...")
            data = CryptoDataFetcherDatabase.fetch()

            if not data:
                logger.error("No data retrieved from the database.")
                return

            # Plot the raw price data before preprocessing
            for coin_id, group in pd.DataFrame(data, columns=['coin_id', 'date', 'price_usd', 'volume_usd']).groupby('coin_id'):
                ModelTrainer.generate_plots(group, None, None, coin_id)  # Plot raw price data

            # Now preprocess the data
            df = CryptoDataPreprocessor.preprocess(data)
            if df is not None:
                ModelTrainer.train_and_evaluate(df, "linear")
                ModelTrainer.train_and_evaluate(df, "xgboost")
            logger.info("✅ Prediction process completed successfully.")
        except Exception as e:
            logger.error(f"Error in the main process: {e}")