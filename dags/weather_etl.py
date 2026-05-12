"""
Structure should look like this:


1. imports

2. python functions 
    extract weather(city, country ,temp ,feels like ,humdity , wind speed, weather description, current timestamp)
    load to postgres

3. dag definition block
    task1 (py operator -> extract weather) 
    task2 (py operator -> load to postgres)
    task3 (bash operator -> dbt run command)

4. dependency block
    task1 -> task2 -> task3

"""
# from dotenv import load_dotenv
import os
import requests
from datetime import datetime
import psycopg2
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")

cities = ["Hyderabad", "Delhi", "Bangalore", "Mumbai", "Chennai"]

def extract_weather(**context):
    weather_data = []
    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            weather_data.append({
                "city": city,
                "country": data["sys"]["country"],
                "temp": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "wind_speed": data["wind"]["speed"],
                "weather_description": data["weather"][0]["description"],
               "timestamp": datetime.utcfromtimestamp(data["dt"]),
            })
    return weather_data

def load_to_postgres(**context):
    weather_data = context['task_instance'].xcom_pull(task_ids='extract_weather')
    conn = psycopg2.connect(
        host="pipeline-db",
        database="weather_db",
        user="pipeline_user",
        password="pipeline_pass"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.weather_raw (
            city VARCHAR,
            country VARCHAR,
            temp FLOAT,
            feels_like FLOAT,
            humidity INT,
            wind_speed FLOAT,
            weather_description VARCHAR,
            timestamp TIMESTAMP,
            ingested_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    for data in weather_data:
        cur.execute(
            "INSERT INTO raw.weather_raw (city, country, temp, feels_like, humidity, wind_speed, weather_description, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                data["city"],
                data["country"],
                data["temp"],
                data["feels_like"],
                data["humidity"],
                data["wind_speed"],
                data["weather_description"],
                data["timestamp"]
            )
        )
    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="weather_etl",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id = "extract_weather",
        python_callable = extract_weather
    )
    task2 = PythonOperator(
        task_id = "load_to_postgres",
        python_callable = load_to_postgres
    )
    task3 = BashOperator(
        task_id = "run_dbt",
        bash_command="dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project && dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project"
    )

    task1 >> task2 >> task3
