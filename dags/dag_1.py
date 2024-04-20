"""Module providing a function fetching XKCD comic data."""

from datetime import timedelta
from typing import Optional
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import requests

URL = 'https://xkcd.com/614/info.0.json'

def _fetch_comic_of_the_day(url=URL, timeout=10) -> Optional[dict]:
    """
    Description:
    Makes an API call with error handling and returns the response content.

    Args:
    url (str): The URL of the API endpoint.
    timeout (int, optional): The timeout in seconds for the request. Defaults to 10.

    Returns:
    dict or None: The response content as a dictionary (JSON) or None if an error occurs.

    Raises:
        requests.exceptions.RequestException: If any error occurs during the request.
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making API call: {e}")
        return None

default_args = {
    'catchup': False,
    'depends_on_past': False,
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=120),
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='xkcd_comics_etl',
    default_args=default_args,
    description='Fetch XKCD comics data and insert it into a database',
    schedule_interval='0 12 * * 1,3,5',
)
# 0: Minute (set to 0 to run at the beginning of the hour)
# 0: Hour (set to 0 to run at midnight)
# *: Day of month (any day)
# *: Month (any month)
# 1,3,5: Days of week (Monday is 1, Wednesday is 3, Friday is 5)

# check_new_comic = HttpSensor(
#     task_id='check_new_comic',
#     http_method='GET',
#     endpoint='https://xkcd.com/info.0.json', 
#     xcom_push=True,  # Pushes response data to XCom for next task
#     response_check=lambda response: int(response.json()['num']) > stored_comic_id,  # Custom check
#     timeout=5,  # Set a timeout for the request
#     dag=dag
# )

with dag:
    fetch_comics_task = PythonOperator(
        task_id='fetch_comics_task',
        python_callable=_fetch_comic_of_the_day,
        dag=dag,

    )
    create_schema = PostgresOperator(
        task_id='create_schema',
        postgres_conn_id='postgres',
        sql= """Create schema if not exists xkcd;""")
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql= """CREATE TABLE if not exists xkcd.comic_data (
            alt TEXT,
            day TEXT,
            img TEXT,
            link text,
            month text,
            news text,
            num integer primary key,
            safe_title text,
            title TEXT,
            transcript text
            );
            """)
    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='postgres',
        sql= """INSERT INTO xkcd.comic_data (num, month, safe_title) values (634, '7', 'test');
            """)

    create_schema >> create_table >> fetch_comics_task >>insert_data