from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import json

LATITUDE=Variable.get("latitude")
LONGITUDE=Variable.get("longitude")
MYSQL_CONN_ID='my_local_mysql'
API_CONN_ID='open_meteo_api'

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

with DAG(dag_id='weather_etl_mysql_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:
    
    @task()
    def extract_weather_data():
        """Extract weather data from Open-Meteo API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')
        ## Build the API endpoint
        ## https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        response=http_hook.run(endpoint)

        if response.status_code==200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data :{response.status_code}")
    
    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        sql_hook=MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        connection=sql_hook.get_conn()
        cursor=connection.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        INSERT INTO weather_data(latitude, longitude,temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s ,%s ,%s ,%s ,%s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        connection.commit()
        cursor.close()

    # DAG Workflow- ETL Pipleline
    weather_data=extract_weather_data()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)

