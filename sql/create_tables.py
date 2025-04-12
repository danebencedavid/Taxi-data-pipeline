import psycopg2

conn = psycopg2.connect(
    dbname="taxi_data_pipeline",
    user="admin",
    password="admin",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS trips_clean (
    tpep_pickup_datetime TEXT,
    tpep_dropoff_datetime TEXT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    passenger_count TEXT,
    PULocationID TEXT,
    DOLocationID TEXT,
    pickup_time TIMESTAMP,
    dropoff_time TIMESTAMP,
    trip_duration_minutes FLOAT,
    valid BOOLEAN
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS trips_dirty (
    tpep_pickup_datetime TEXT,
    tpep_dropoff_datetime TEXT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    passenger_count TEXT,
    PULocationID TEXT,
    DOLocationID TEXT,
    pickup_time TIMESTAMP,
    dropoff_time TIMESTAMP,
    trip_duration_minutes FLOAT,
    valid BOOLEAN
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS zones (
    location_id INTEGER PRIMARY KEY,
    zone TEXT,
    borough TEXT
);
""")

conn.commit()
cur.close()
conn.close()

print("✅ Tables created successfully.")
