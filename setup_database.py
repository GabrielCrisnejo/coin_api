import os
import psycopg2
import json
import argparse
from psycopg2 import sql

# Configuración de la conexión a PostgreSQL
DB_PARAMS = {
    "dbname": "crypto_db",
    "user": "gcrisnejo",
    "password": "gcrisnejo",
    "host": "localhost",
    "port": "5432"
}

SQL_FILES = ["01_create_raw_data_table.sql", "02_create_aggregated_data_table.sql"]
JSON_FILES_PATH = './crypto_data'

def execute_sql_file(cursor, file_path):
    """ Ejecuta un archivo SQL en la base de datos """
    with open(file_path, "r") as file:
        sql_commands = file.read()
        cursor.execute(sql.SQL(sql_commands))

def insert_raw_data(cursor, data, coin_id, date):
    """ Inserta los datos crudos en la tabla raw_crypto_data, ignorando duplicados """
    try:
        cursor.execute("""
            INSERT INTO raw_crypto_data (coin_id, date, price_usd, raw_json)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (coin_id, date) DO NOTHING
        """, (
            coin_id,
            date,
            data['market_data']['current_price']['usd'],  # Precio en USD
            json.dumps(data)  # Guardamos el JSON completo
        ))
        print(f"✅ Datos de {coin_id} para {date} insertados en raw_crypto_data.")
    except Exception as e:
        print(f"❌ Error al insertar datos crudos: {e}")

def insert_aggregated_data(cursor, data, coin_id, year, month, day):
    """ Inserta o actualiza los datos agregados en la tabla aggregated_crypto_data """
    try:
        # Consultar los valores actuales de max_price y min_price
        cursor.execute("""
            SELECT max_price, min_price
            FROM aggregated_crypto_data
            WHERE coin_id = %s AND year = %s AND month = %s
        """, (coin_id, year, month))
        existing_data = cursor.fetchone()

        new_price = data['market_data']['current_price']['usd']
        
        if existing_data:
            max_price, min_price = existing_data
            # Actualizamos los precios si es necesario
            if new_price > max_price:
                max_price = new_price
            if new_price < min_price:
                min_price = new_price

            cursor.execute("""
                UPDATE aggregated_crypto_data
                SET max_price = %s, min_price = %s
                WHERE coin_id = %s AND year = %s AND month = %s
            """, (max_price, min_price, coin_id, year, month))
            print(f"🔄 Datos de {coin_id} para {year}-{month}-{day} actualizados en aggregated_crypto_data.")
        else:
            # Insertamos nuevos datos si no existen
            cursor.execute("""
                INSERT INTO aggregated_crypto_data (coin_id, year, month, max_price, min_price)
                VALUES (%s, %s, %s, %s, %s)
            """, (coin_id, year, month, new_price, new_price))
            print(f"✅ Datos agregados de {coin_id} para {year}-{month}-{day} insertados en aggregated_crypto_data.")
    except Exception as e:
        print(f"❌ Error al insertar o actualizar datos agregados: {e}")


def setup_database(store_data):
    """ Ejecuta los scripts SQL para crear tablas y llena las tablas con datos JSON si se solicita """
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = True
        cursor = conn.cursor()

        # Ejecutar los archivos SQL para crear tablas
        for sql_file in SQL_FILES:
            if os.path.exists(sql_file):
                print(f"📂 Ejecutando {sql_file}...")
                execute_sql_file(cursor, sql_file)
                print(f"✅ {sql_file} ejecutado con éxito.")
            else:
                print(f"⚠️ Archivo {sql_file} no encontrado.")

        # Procesar y cargar los archivos JSON si se solicita
        for json_file in os.listdir(JSON_FILES_PATH):
            if json_file.endswith('.json'):
                file_path = os.path.join(JSON_FILES_PATH, json_file)
                with open(file_path, 'r') as f:
                    data = json.load(f)

                    # Extraer coin_id, year, month, day desde el nombre del archivo
                    try:
                        coin_id, date_str = json_file.replace('.json', '').split('_')
                        year, month, day = date_str.split('-')

                        # Si se requiere almacenar en la base de datos
                        if store_data:
                            # Insertar los datos crudos
                            insert_raw_data(cursor, data, coin_id, f'{year}-{month}-{day}')

                            # Insertar o actualizar los datos agregados
                            insert_aggregated_data(cursor, data, coin_id, year, month, day)
                        else:
                            print(f"⚠️ No se almacenarán los datos de {coin_id} para {year}-{month}-{day}.")
                        
                        # Imprimir mensaje correctamente formateado
                        print(f"🔄 Datos de {coin_id} para {year}-{month}-{day} actualizados en aggregated_crypto_data.")

                    except Exception as e:
                        print(f"⚠️ Error al parsear el nombre del archivo {json_file}: {e}")

        cursor.close()
        conn.close()
        print("✅ Configuración y carga de datos completadas.")
    except Exception as e:
        print(f"❌ Error en la configuración de la base de datos: {e}")


def parse_args():
    """ Función para manejar los argumentos de la línea de comandos """
    parser = argparse.ArgumentParser(description="Carga de datos de criptomonedas en la base de datos.")
    parser.add_argument(
        '--store',
        action='store_true',
        help="Habilita el almacenamiento de datos en la base de datos PostgreSQL"
    )
    return parser.parse_args()

if __name__ == "__main__":
    # Parsear argumentos de la línea de comandos
    args = parse_args()
    
    # Configuración y carga de datos, si se solicita almacenar en la base de datos
    setup_database(args.store)
