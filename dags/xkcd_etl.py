import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from data_collection.fetch_api import _fetch_comic_of_the_day

local_tz = pendulum.timezone('US/Eastern')
now_eastern = pendulum.now(tz=local_tz)
start_date_eastern = now_eastern - timedelta(days=1) # equivalent to days_ago(1)

default_args = {
    'catchup': False,
    'depends_on_past': False,
    'owner': 'airflow',
    'retries': 7,
    'retry_delay': timedelta(minutes=120),
    'start_date': start_date_eastern
}

dag = DAG(
    dag_id='dag_xkcd_etl',
    default_args=default_args,
    schedule_interval='0 10 * * 1,3,5',
)
with dag:
    task_fetch_comics = PythonOperator(
        task_id='task_fetch_comics',
        python_callable=_fetch_comic_of_the_day,
        provide_context=True,
        do_xcom_push=True,
    )
    insert_data = PostgresOperator(
        task_id='task_insert_data',
        postgres_conn_id='postgres',
        sql="""
        INSERT INTO xkcd.comic (alt, day, img, link, month, news, num, safe_title, title, transcript, year)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (num) DO UPDATE SET
            alt = EXCLUDED.alt,
            day = EXCLUDED.day,
            img = EXCLUDED.img,
            link = EXCLUDED.link,
            month = EXCLUDED.month,
            news = EXCLUDED.news,
            safe_title = EXCLUDED.safe_title,
            title = EXCLUDED.title,
            transcript = EXCLUDED.transcript,
            year = EXCLUDED.year;
    """,
    parameters=[
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['alt'] }}",
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['day'] }}",
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['img'] }}",
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['link'] }}",
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['month'] }}",
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['news'] }}",
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['num'] }}",
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['safe_title'] }}",
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['title'] }}",
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['transcript'] }}",
        "{{ task_instance.xcom_pull(task_ids='task_fetch_comics')['year'] }}"],
    )

    task_fetch_comics >> insert_data