import psycopg2
import pandas as pd
import os

DB_CONFIG = {
    'host': 'localhost',
    'database': 'taxi_data_pipeline',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

OUTPUT_FILENAME = f"C:/Users/daneb/Documents/taxi-data-pipeline/data/data_from_db.csv"

def get_postgres_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error: {e}")
        return None

def export_all_trips_to_csv(conn, output_file):
    try:
        print(f"Exporting: {output_file}...")
        query = "SELECT * FROM trips;"
        df = pd.read_sql(query, conn)
        df.to_csv(output_file, index=False)
        print(f"Export done {len(df)} sort ide: {output_file}")
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    conn = get_postgres_connection()
    if conn is None:
        exit(1)

    df = export_all_trips_to_csv(conn, OUTPUT_FILENAME)

    conn.close()

    if df is not None:
        print("\nSample:")
        print(df.head())