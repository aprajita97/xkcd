from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define the DAG schedule and default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'dbt_run',
    default_args=default_args,
    schedule_interval=None,
)

# Define the task that runs the dbt run command
with dag:
    dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dags/dbt_xkcd && dbt run',
    dag=dag,
    )
    dbt_run
