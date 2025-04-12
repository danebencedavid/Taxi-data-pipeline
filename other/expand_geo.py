import geopandas as gpd
import pandas as pd

# Read the GeoJSON file
zones_gdf = gpd.read_file('C:/Users/daneb/Documents/taxi-data-pipeline/data/NYC Taxi Zones.geojson')
zones_gdf = zones_gdf.to_crs("EPSG:4326")
zones_gdf = zones_gdf[['location_id', 'zone', 'borough']]

# Read the CSV file
trips_df = pd.read_csv('C:/Users/daneb/Documents/taxi-data-pipeline/data/sample_2024-12.csv')

# Convert location_id to integer in zones_gdf to match trips_df's PULocationID
zones_gdf['location_id'] = zones_gdf['location_id'].astype(int)

# First merge for pickup locations
trips_df = trips_df.merge(
    zones_gdf,
    how='left',
    left_on='PULocationID',
    right_on='location_id'
).rename(columns={'zone': 'PU_Zone', 'borough': 'PU_Borough'})

# Second merge for dropoff locations
trips_df = trips_df.merge(
    zones_gdf,
    how='left',
    left_on='DOLocationID',
    right_on='location_id'
).rename(columns={'zone': 'DO_Zone', 'borough': 'DO_Borough'})

# Optionally drop the extra location_id columns if you don't need them
trips_df = trips_df.drop(columns=['location_id_x', 'location_id_y'])

trips_df.to_csv('C:/Users/daneb/Documents/taxi-data-pipeline/data/expanded_sample_2024-12.csv',index=False)