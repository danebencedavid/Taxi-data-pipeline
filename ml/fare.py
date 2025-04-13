import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

df = pd.read_csv("C:/Users/daneb/Documents/taxi-data-pipeline/data/data_from_db.csv")

print(df.columns)

df['pickup_time'] = pd.to_datetime(df['pickup_time'])

df['pickup_hour'] = df['pickup_time'].dt.hour
df['pickup_dayofweek'] = df['pickup_time'].dt.dayofweek
df['pickup_month'] = df['pickup_time'].dt.month

X = df[['trip_distance', 'trip_duration_minutes', 'pulocationid', 'dolocationid', 'passenger_count', 'pickup_hour', 'pickup_dayofweek', 'pickup_month']]
y = df['fare_amount']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestRegressor(random_state=42)
model.fit(X_train, y_train)

predictions = model.predict(X_test)

mse = mean_squared_error(y_test, predictions)
r2 = r2_score(y_test, predictions)

print(f"Mean Squared Error (MSE): {mse:.2f}")
print(f"R-squared (R2): {r2*100:.2f}%")
