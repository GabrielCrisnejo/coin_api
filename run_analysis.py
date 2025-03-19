import psycopg2

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

def run_sql_queries():
    """Ejecuta las consultas de análisis guardadas en un archivo SQL."""
    with open("analysis_queries.sql", "r") as file:
        queries = file.read().split(";")  # Divide las consultas si hay varias

    for idx, query in enumerate(queries, start=1):
        query = query.strip()
        if query:  # Evitar líneas vacías
            if idx == 1:
                print("📊 Ejecutando consulta: Promedio de precio por moneda y mes")
            elif idx == 2:
                print("📊 Ejecutando consulta: Aumento promedio de precio después de caídas consecutivas")

            results = execute_sql_query(query)
            if results:
                if idx == 1:
                    print("📅 Promedio de precio por moneda y mes (en USD):")
                    for row in results:
                        coin, year, month, avg_price = row
                        print(f"  - {coin} | Año: {year} | Mes: {month} | Promedio: ${avg_price:.4f} USD")
                elif idx == 2:
                    if results[0][0] == 'No se encontraron caídas consecutivas de más de 3 días':
                        print("ℹ️ No se encontraron monedas con caídas consecutivas de más de 3 días.")
                    else:
                        print("📈 Aumento promedio del precio después de caídas consecutivas (en USD):")
                        for row in results:
                            coin, avg_price_increase, market_cap_usd = row
                            
                            # Ignorar registros con valores None
                            if avg_price_increase is None or market_cap_usd is None:
                                continue
                            
                            # Redondear los valores después de convertir a float
                            try:
                                avg_price_increase = round(float(avg_price_increase), 2)
                                market_cap_usd = round(float(market_cap_usd), 4)
                            except (ValueError, TypeError):
                                print(f"❌ Error: Los valores de {coin} no son válidos para formatear: Aumento Promedio: {avg_price_increase}, Market Cap: {market_cap_usd}")
                                continue
                            
                            # Verificar si los valores son numéricos después de la conversión
                            if isinstance(avg_price_increase, (int, float)) and isinstance(market_cap_usd, (int, float)):
                                print(f"  - {coin} | Aumento promedio del precio: ${avg_price_increase:.2f} USD | Market Cap: ${market_cap_usd:.4f} USD")
                            else:
                                print(f"❌ Error: Los valores de {coin} son inválidos para formatear: Aumento Promedio: {avg_price_increase}, Market Cap: {market_cap_usd}")
            else:
                print(f"❌ No se encontraron resultados para la consulta {idx}.")

if __name__ == "__main__":
    run_sql_queries()
