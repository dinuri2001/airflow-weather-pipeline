import requests
import psycopg2
from datetime import datetime


def create_table():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(50),
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            extracted_at TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    conn.close()


def extract_and_store():
    url = "https://api.open-meteo.com/v1/forecast?latitude=6.9271&longitude=79.8612&current_weather=true"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        current_weather = data.get("current_weather", {})
        temperature = current_weather.get("temperature")
        windspeed = current_weather.get("windspeed")
        winddirection = current_weather.get("winddirection")
        weathercode = current_weather.get("weathercode")
        extracted_at = datetime.now()

        if temperature is None or windspeed is None or winddirection is None or weathercode is None:
            raise ValueError("Missing weather fields in API response")

        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO weather_data (
                city, temperature, windspeed, winddirection, weathercode, extracted_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, ("Colombo", temperature, windspeed, winddirection, weathercode, extracted_at))

        conn.commit()
        cur.close()
        conn.close()

        print("Weather data extracted and stored successfully.")

    except Exception as e:
        print(f"Error occurred: {e}")
        raise


if __name__ == "__main__":
    create_table()
    extract_and_store()
