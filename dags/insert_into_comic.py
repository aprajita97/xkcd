from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from fetch_api import _fetch_comic_of_the_day

default_args = {
    'catchup': False,
    'depends_on_past': False,
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=120),
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='xkcd_comics_extract_and_load',
    default_args=default_args,
    description='Fetch XKCD comics data and insert it into a database',
    schedule_interval='0 12 * * 1,3,5',
)
with dag:
    fetch_comics_task = PythonOperator(
        task_id='fetch_comics_task',
        python_callable=_fetch_comic_of_the_day,
        dag=dag,
        provide_context=True,
        do_xcom_push=True,

    )
    
    insert_data = PostgresOperator(
        task_id='insert_data',
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
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['alt'] }}",
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['day'] }}",
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['img'] }}",
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['link'] }}",
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['month'] }}",
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['news'] }}",
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['num'] }}",
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['safe_title'] }}",
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['title'] }}",
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['transcript'] }}",
        "{{ task_instance.xcom_pull(task_ids='fetch_comics_task')['year'] }}"
    ],
        dag=dag,
    )

    fetch_comics_task >> insert_data