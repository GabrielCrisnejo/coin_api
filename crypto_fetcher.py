import requests
import logging
import json
import os
from datetime import datetime, timedelta
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuración de logging
LOG_FILE = "crypto_fetcher.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()  # También muestra los logs en consola
    ]
)

# Carpeta para guardar los datos
DATA_DIR = "crypto_data"
os.makedirs(DATA_DIR, exist_ok=True)

# Lista de criptomonedas a consultar
COINS = ["bitcoin", "ethereum", "cardano"]

# Limite de concurrencia
CONCURRENT_REQUESTS = 10  # Limite de peticiones simultáneas
REQUESTS_PER_MINUTE = 30  # Límite de solicitudes por minuto de la API

def fetch_crypto_data(coin_id, date):
    """ Descarga datos históricos de criptomonedas para una fecha específica. """
    try:
        formatted_date = date.strftime("%d-%m-%Y")  # Formato requerido por la API
        API_URL = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
        params = {"date": formatted_date}
        response = requests.get(API_URL, params=params)

        if response.status_code == 200:
            data = response.json()
            filename = os.path.join(DATA_DIR, f"{coin_id}_{date.strftime('%Y-%m-%d')}.json")

            with open(filename, "w") as file:
                json.dump(data, file, indent=4)

            logging.info(f"✅ Datos guardados en {filename}")
        elif response.status_code == 429:  # Error 429 - Rate Limit Exceeded
            logging.error(f"❌ Error 429 para {coin_id} en {formatted_date}: {response.json()}")
            logging.info("Esperando 10 segundos antes de reintentar...")
            time.sleep(10)  # Espera 10 segundos antes de intentar nuevamente
            fetch_crypto_data(coin_id, date)  # Vuelve a intentar la solicitud
        else:
            logging.error(f"❌ Error {response.status_code} para {coin_id} en {formatted_date}: {response.json()}")

    except requests.RequestException as e:
        logging.error(f"🚨 Fallo en la solicitud para {coin_id} en {formatted_date}: {e}")

def process_single_date(coin, single_date):
    """ Procesa un solo día de datos para una criptomoneda específica. """
    fetch_crypto_data(coin, single_date)

def bulk_reprocess_data(start_date, end_date):
    """ Procesa datos para un rango de fechas usando concurrencia. """
    logging.info(f"📅 Iniciando procesamiento en bloque desde {start_date} hasta {end_date}")
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    total_days = (end_date - start_date).days + 1
    with ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
        futures = []
        request_count = 0  # Contador de solicitudes realizadas
        # Para cada fecha y criptomoneda, añadimos la tarea de procesamiento
        for single_date in tqdm((start_date + timedelta(n) for n in range(total_days)), total=total_days, desc="Procesando fechas"):
            for coin in COINS:
                futures.append(executor.submit(process_single_date, coin, single_date))
                request_count += 1

                # Si alcanzamos el límite de 30 solicitudes, esperamos 60 segundos
                if request_count >= REQUESTS_PER_MINUTE:
                    logging.info(f"Limite de {REQUESTS_PER_MINUTE} solicitudes alcanzado. Esperando 60 segundos...")
                    time.sleep(60)  # Esperar 1 minuto para no superar el límite
                    request_count = 0  # Reiniciar el contador de solicitudes

        # Esperar a que todos los futuros terminen
        for future in as_completed(futures):
            future.result()  # Esto lanzará cualquier excepción que haya ocurrido en el hilo

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Crypto Data Fetcher")
    parser.add_argument("--start-date", help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Fecha de fin (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.start_date and args.end_date:
        bulk_reprocess_data(args.start_date, args.end_date)
    else:
        today = "2024-03-01"  # Fecha específica
        bulk_reprocess_data(today, today)  # Procesa solo esa fecha
