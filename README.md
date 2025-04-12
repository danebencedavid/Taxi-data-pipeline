README - NYC Taxi Data Pipeline Dashboard

This project is a data pipeline and visualization dashboard for New York City (NYC) taxi trips.
It involves streaming real-time taxi trip data from a CSV file to Kafka, processing this data using Apache Spark, and visualizing the results through a Streamlit dashboard.

Components:
1. Kafka Producer (CSV to Kafka): Streams NYC taxi trip data from a CSV file into a Kafka topic.
2. Spark Streaming: Consumes data from the Kafka topic, processes it, performs data quality checks, joins it with geospatial data (NYC taxi zones), and stores the results in a PostgreSQL database.
3. Streamlit Dashboard: A web application that visualizes processed taxi trip data, including metrics, trip details, and geospatial data on maps.


TODO: Predicting trip prices and tips.

![Untitled Diagram (1)](https://github.com/user-attachments/assets/04125268-cbed-4408-b4f2-7df6317c81f4)

