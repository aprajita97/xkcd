from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_init',
    default_args=default_args,
    schedule_interval=None,
)

with dag:
    dbt_init = BashOperator(
        task_id='dbt_init',
        bash_command='cd /home/airflow && mkdir -p .dbt && \
            cp /opt/airflow/dags/.dbt/profiles.yml /home/airflow/.dbt/',
    )

    dbt_init