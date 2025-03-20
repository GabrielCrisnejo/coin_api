import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from datetime import datetime, timedelta
import warnings
import holidays
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np
from sklearn.metrics import mean_absolute_error, r2_score
import xgboost as xgb

# Suprimir los warnings de deprecación
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Configuración de la conexión a PostgreSQL
DB_PARAMS = {
    "dbname": "crypto_db",
    "user": "gcrisnejo",
    "password": "gcrisnejo",
    "host": "localhost",
    "port": "5432"
}

def execute_sql_query(query):
    """Ejecuta una consulta SQL en la base de datos y devuelve los resultados."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        print(f"❌ Error al ejecutar la consulta: {e}")
        return None

# Obtener la fecha de hoy y la fecha de hace 30 días
today = datetime.today()
thirty_days_ago = today - timedelta(days=30)

# Consulta SQL para obtener los datos de las criptomonedas
query = f"""
SELECT coin_id, date, price_usd, volume_usd
FROM raw_crypto_data
WHERE date >= '{thirty_days_ago.strftime('%Y-%m-%d')}'
ORDER BY coin_id, date;
"""

# Ejecutar la consulta y obtener los resultados
data = execute_sql_query(query)

# Convertir los resultados a un DataFrame de pandas
df = pd.DataFrame(data, columns=['coin_id', 'date', 'price_usd', 'volume_usd'])

# Convertir la columna 'date' a tipo datetime
df['date'] = pd.to_datetime(df['date'])

# Obtener todas las monedas únicas en los datos
coins = df['coin_id'].unique()

# Añadir columnas para la tendencia general y la varianza de los últimos 7 días
def calculate_trend_and_variance(group):
    group['price_diff_7_days'] = group['price_usd'].diff(7)
    group['variance_7_days'] = group['price_usd'].rolling(window=7).var()
    return group

df = df.groupby('coin_id', group_keys=False).apply(calculate_trend_and_variance)

# Generar columnas con el precio de los últimos 7 días y el precio del día siguiente
for i in range(1, 8):
    df[f'price_lag_{i}'] = df.groupby('coin_id')['price_usd'].shift(i)

df['price_target'] = df.groupby('coin_id')['price_usd'].shift(-1)

# Agregar características temporales
df['day_of_week'] = df['date'].dt.dayofweek
df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
df['week_of_year'] = df['date'].dt.isocalendar().week
df['month'] = df['date'].dt.month

# Agregar información sobre días festivos en China y EE.UU.
us_holidays = holidays.US()
china_holidays = holidays.China()

df['is_us_holiday'] = df['date'].isin(us_holidays).astype(int)
df['is_china_holiday'] = df['date'].isin(china_holidays).astype(int)

# Funciones para volumen
def calculate_volume_7_days(group):
    """ Calcular el promedio del volumen de los últimos 7 días """
    group['volume_avg_7_days'] = group['volume_usd'].rolling(window=7).mean()
    return group

def calculate_volume_ratio(group):
    """ Calcular el ratio del volumen del día actual con el promedio de los últimos 7 días """
    group['volume_ratio'] = group['volume_usd'].astype(float) / group['volume_avg_7_days'].astype(float)
    return group

def calculate_cumulative_volume(group):
    """ Calcular el volumen acumulado hasta el día actual """
    group['volume_cumulative'] = group['volume_usd'].cumsum()
    return group

# Aplicar las funciones de volumen
df = df.groupby('coin_id', group_keys=False).apply(calculate_volume_7_days)
df = df.groupby('coin_id', group_keys=False).apply(calculate_volume_ratio)
df = df.groupby('coin_id', group_keys=False).apply(calculate_cumulative_volume)

# Eliminar filas con NaN para las características de los 7 días
df = df.dropna(subset=[f'price_lag_{i}' for i in range(1, 8)])

# Eliminar filas donde no se puede predecir el próximo precio (último día para cada moneda)
df = df.dropna(subset=['price_target'])

# Convertir las columnas de precios retrasados a float
for i in range(1, 8):
    df[f'price_lag_{i}'] = pd.to_numeric(df[f'price_lag_{i}'], errors='coerce')

# Función para entrenamiento y evaluación del modelo de regresión lineal sin regularización
def train_linear_model(df):
    print("🚀 Entrenando el modelo de regresión lineal sin regularización...")
    
    # Seleccionar las características y la variable objetivo
    features = [f'price_lag_{i}' for i in range(1, 8)]  # Las características de los 7 días anteriores
    X = df[features]
    y = df['price_target']

    # Dividir en conjunto de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Entrenar el modelo de regresión lineal sin regularización
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Predecir los precios usando el modelo
    y_pred = model.predict(X_test)

    # Calcular las métricas
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Imprimir los resultados
    print(f"🎯 Modelo de regresión lineal sin regularización:")
    print(f"Mean Squared Error (MSE): {mse}")
    print(f"Mean Absolute Error (MAE): {mae}")
    print(f"R-squared (R²): {r2}")

# Entrenar y evaluar el modelo de regresión lineal sin regularización
train_linear_model(df)

# Función para entrenamiento y evaluación del modelo de regresión lineal con Ridge
def train_ridge_model(df):
    print("🚀 Entrenando el modelo de regresión lineal con Ridge...")
    
    # Seleccionar las características y la variable objetivo
    features = [f'price_lag_{i}' for i in range(1, 8)]  # Las características de los 7 días anteriores
    X = df[features]
    y = df['price_target']

    # Dividir en conjunto de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Entrenar el modelo de regresión lineal con Ridge
    model = Ridge(alpha=1.0)  # Puedes ajustar el parámetro alpha para regularizar
    model.fit(X_train, y_train)

    # Predecir los precios usando el modelo
    y_pred = model.predict(X_test)

    # Calcular las métricas
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Imprimir los resultados
    print(f"🎯 Modelo de regresión lineal con Ridge:")
    print(f"Mean Squared Error (MSE): {mse}")
    print(f"Mean Absolute Error (MAE): {mae}")
    print(f"R-squared (R²): {r2}")

# Entrenar y evaluar el modelo de regresión lineal con Ridge
train_ridge_model(df)

# Función para entrenamiento y evaluación del modelo de regresión lineal con Lasso
def train_lasso_model(df):
    print("🚀 Entrenando el modelo de regresión lineal con Lasso...")
    
    # Seleccionar las características y la variable objetivo
    features = [f'price_lag_{i}' for i in range(1, 8)]  # Las características de los 7 días anteriores
    X = df[features]
    y = df['price_target']

    # Dividir en conjunto de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Entrenar el modelo de regresión lineal con Lasso
    model = Lasso(alpha=0.1)  # Puedes ajustar el parámetro alpha para regularizar
    model.fit(X_train, y_train)

    # Predecir los precios usando el modelo
    y_pred = model.predict(X_test)

    # Calcular las métricas
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Imprimir los resultados
    print(f"🎯 Modelo de regresión lineal con Lasso:")
    print(f"Mean Squared Error (MSE): {mse}")
    print(f"Mean Absolute Error (MAE): {mae}")
    print(f"R-squared (R²): {r2}")

# Entrenar y evaluar el modelo de regresión lineal con Lasso
train_lasso_model(df)

# Función para entrenamiento y evaluación del modelo XGBoost
def train_xgboost_model(df):
    print("🚀 Entrenando el modelo XGBoost...")
    
    # Seleccionar las características y la variable objetivo
    features = [f'price_lag_{i}' for i in range(1, 8)]  # Las características de los 7 días anteriores
    X = df[features]
    y = df['price_target']

    # Dividir en conjunto de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Crear el modelo XGBoost
    model = xgb.XGBRegressor(n_estimators=100, random_state=42)

    # Entrenar el modelo
    model.fit(X_train, y_train)

    # Predecir los precios usando el modelo
    y_pred = model.predict(X_test)

    # Calcular las métricas
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Imprimir los resultados
    print(f"🎯 Modelo XGBoost:")
    print(f"Mean Squared Error (MSE): {mse}")
    print(f"Mean Absolute Error (MAE): {mae}")
    print(f"R-squared (R²): {r2}")

# Entrenar y evaluar el modelo XGBoost
train_xgboost_model(df)
