import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import streamlit_folium
import folium
import re
from shapely import wkt
from shapely.geometry import MultiPolygon

st.set_page_config(
    page_title="NYC Taxi Dashboard",
    page_icon="🚖",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_db_connection():
    return create_engine("postgresql+psycopg2://postgres:postgres@localhost/taxi_data_pipeline")

@st.cache_data(ttl=300)
def load_all_trip_data():
    conn = get_db_connection()
    query = """
    SELECT
        tpep_pickup_datetime AS pickup_time,
        tpep_dropoff_datetime AS dropoff_time,
        trip_distance,
        fare_amount,
        tip_amount,
        passenger_count,
        CAST(pulocationid AS INTEGER) AS pulocationid,
        CAST(dolocationid AS INTEGER) AS dolocationid,
        trip_duration_minutes,
        valid,
        pu_zone,
        pu_borough,
        do_zone,
        do_borough
    FROM trips
    WHERE valid = TRUE
    """
    return pd.read_sql(query, conn)

@st.cache_data(ttl=300)
def load_location_data(csv_path="data/taxi_zones.csv"):
    try:
        df_locations = pd.read_csv(csv_path)
        df_locations.rename(columns={'LocationID': 'locationid'}, inplace=True)
        df_locations.set_index('locationid', inplace=True)

        latitudes = []
        longitudes = []
        for geom_str in df_locations['the_geom']:
            try:
                polygon = wkt.loads(geom_str)
                if polygon.geom_type == 'MultiPolygon':
                    outer_polygon = polygon.convex_hull
                    centroid = outer_polygon.centroid
                else:
                    centroid = polygon.centroid
                latitudes.append(centroid.y)
                longitudes.append(centroid.x)
            except Exception as e:
                latitudes.append(None)
                longitudes.append(None)

        df_locations['latitude'] = latitudes
        df_locations['longitude'] = longitudes
        return df_locations[['latitude', 'longitude']]
    except FileNotFoundError:
        st.error(f"Nem található a fájl: {csv_path}")
        return pd.DataFrame()

st.title("🚖 NYC Taxi Dashboard")

try:
    df_trips = load_all_trip_data()
    df_locations = load_location_data()

    st.sidebar.title("Filters")

    borough_options = df_trips['pu_borough'].unique()
    selected_boroughs = st.sidebar.multiselect("Pickup Borough Filter", borough_options)

    dropoff_zone_options = df_trips['do_zone'].unique()
    selected_dropoff_zones = st.sidebar.multiselect("Dropoff Zone Filter", dropoff_zone_options)

    filtered_df = df_trips
    if selected_boroughs:
        filtered_df = filtered_df[filtered_df['pu_borough'].isin(selected_boroughs)]
    if selected_dropoff_zones:
        filtered_df = filtered_df[filtered_df['do_zone'].isin(selected_dropoff_zones)]

    tab1, tab2, tab3 = st.tabs(["Data", "Metrics", "Maps"])

    with tab1:
        st.subheader("Filtered Taxi Trips")
        st.dataframe(filtered_df)

    with tab2:
        st.subheader("Metrics")
        total_trips = len(filtered_df)
        avg_distance = filtered_df['trip_distance'].mean() if not filtered_df.empty else 0
        avg_fare = filtered_df['fare_amount'].mean() if not filtered_df.empty else 0
        avg_tip = filtered_df['tip_amount'].mean() if not filtered_df.empty else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Trips", f"{total_trips:,}")
        col2.metric("Avg Distance", f"{avg_distance:.2f} mi")
        col3.metric("Avg Fare", f"${avg_fare:.2f}")
        col4.metric("Avg Tip", f"${avg_tip:.2f}")

    with tab3:
        st.subheader("Trip Locations")

        if not filtered_df.empty and not df_locations.empty:
            filtered_df['pulocationid'] = pd.to_numeric(filtered_df['pulocationid'], errors='coerce').astype('Int64')
            filtered_df['dolocationid'] = pd.to_numeric(filtered_df['dolocationid'], errors='coerce').astype('Int64')

            pickup_locations = filtered_df.merge(df_locations, left_on='pulocationid', right_index=True, how='left')
            pickup_locations.rename(columns={'latitude': 'pu_latitude', 'longitude': 'pu_longitude'}, inplace=True)

            dropoff_locations = pickup_locations.merge(df_locations, left_on='dolocationid', right_index=True, how='left')
            dropoff_locations.rename(columns={'latitude': 'do_latitude', 'longitude': 'do_longitude'}, inplace=True)

            valid_locations = dropoff_locations.dropna(subset=['pu_latitude', 'pu_longitude', 'do_latitude', 'do_longitude'])

            if not valid_locations.empty:
                m = folium.Map(location=[40.7128, -74.0060], zoom_start=10)

                for index, row in valid_locations.head(100).iterrows():
                    if pd.notna(row['pu_latitude']) and pd.notna(row['pu_longitude']):
                        folium.Marker([row['pu_latitude'], row['pu_longitude']], popup=f"Pickup Zone: {row['pu_zone']}").add_to(m)
                    if pd.notna(row['do_latitude']) and pd.notna(row['do_longitude']):
                        folium.Marker([row['do_latitude'], row['do_longitude']], popup=f"Dropoff Zone: {row['do_zone']}", icon=folium.Icon(color='red')).add_to(m)

                streamlit_folium.folium_static(m)
            else:
                st.warning("No valid pickup or dropoff coordinates found for the filtered data.")
        elif filtered_df.empty:
            st.info("No trip data available based on the current filters to display on the map.")
        else:
            st.warning("Location data (taxi_zones.csv) could not be loaded or processed.")

except Exception as e:
    st.error(f"Error: {str(e)}")