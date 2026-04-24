from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Dinuri",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_pipeline_dag",
    default_args=default_args,
    description="Daily weather data pipeline using Airflow and PostgreSQL",
    start_date=datetime(2026, 4, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "api", "postgresql"],
) as dag:

    extract_weather_task = BashOperator(
        task_id="extract_weather_data",
        bash_command="python /opt/airflow/scripts/extract_weather.py"
    )

    extract_weather_task
