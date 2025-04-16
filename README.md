# NYC Taxi Trips in Real-Time! 🗽🚕💨

This project is a  data pipeline designed to ingest, process, and visualize New York City (NYC) taxi trip data.

Key Components:

-  📤 Kafka Producer (CSV to Kafka): Simulates real-time data streaming by pushing NYC taxi trip data from a CSV file into a dedicated Kafka topic.
-  ⚙️ Apache Spark Streaming: The powerful engine that consumes the live data stream from Kafka. It performs crucial operations:
    -  🧹 Data Quality Checks: Ensures data integrity by identifying and handling inconsistencies.
    -  📍 Geospatial Enrichment: Joins trip data with NYC taxi zone information, adding valuable location context.
    -  💾 PostgreSQL Sink: Persists the clean and enriched data into a robust PostgreSQL database.
- 📊 Streamlit Dashboard: A user-friendly web application that brings the processed data to life through interactive visualizations    

Next Steps:
- 📈 Implementing more advanced analytical features and visualizations.
