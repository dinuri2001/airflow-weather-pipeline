import pandas as pd
import psycopg2
import matplotlib.pyplot as plt

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="airflow",
    user="airflow",
    password="airflow"
)

query = "SELECT * FROM weather_data ORDER BY extracted_at;"
df = pd.read_sql(query, conn)
conn.close()

print(df)

plt.figure(figsize=(10, 5))
plt.plot(df["extracted_at"], df["temperature"], marker="o")
plt.title("Temperature Trend Over Time")
plt.xlabel("Extraction Time")
plt.ylabel("Temperature (°C)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 5))
plt.plot(df["extracted_at"], df["windspeed"], marker="o")
plt.title("Wind Speed Trend Over Time")
plt.xlabel("Extraction Time")
plt.ylabel("Wind Speed")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
