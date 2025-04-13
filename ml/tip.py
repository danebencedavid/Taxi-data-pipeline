import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib
import matplotlib.pyplot as plt
import psycopg2


def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="taxi_data_pipeline",
        user="postgres",
        password="admin"
    )

def load_tip_data():
    query = """
    SELECT 
        fare_amount,
        trip_distance,
        trip_duration_minutes,
        EXTRACT(HOUR FROM pickup_time) AS pickup_hour,
        EXTRACT(DOW FROM pickup_time) AS day_of_week,
        CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END AS will_tip,
        pu_borough
    FROM trips
    WHERE fare_amount > 0 
      AND trip_distance > 0
      AND trip_duration_minutes > 0
    """
    conn = get_db_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def prepare_features(df):
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['is_night'] = df['pickup_hour'].between(0, 5).astype(int)

    df = pd.get_dummies(df, columns=['pu_borough'], drop_first=True)

    return df


def tip_prediction_workflow():
    df = load_tip_data()
    df = prepare_features(df)

    X = df.drop('will_tip', axis=1)
    y = df['will_tip']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('classifier', RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            class_weight='balanced'
        ))
    ])

    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    print("Classification Report:")
    print(classification_report(y_test, y_pred))
    print(f"Accuracy: {accuracy_score(y_test, y_pred):.2f}")

    importances = pipeline.named_steps['classifier'].feature_importances_
    features = X.columns
    plt.figure(figsize=(10, 6))
    plt.barh(features, importances)
    plt.title("Feature Importance for Tip Prediction")
    plt.tight_layout()
    plt.show()

    joblib.dump(pipeline, 'tip_predictor.pkl')
    print("Model saved as tip_predictor.pkl")


if __name__ == "__main__":
    tip_prediction_workflow()


