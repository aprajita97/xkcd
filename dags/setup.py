from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'catchup': False,
    'depends_on_past': False,
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='dag_setup',
    default_args=default_args,
    schedule_interval=None,
)

with dag:
    dbt_initialization = BashOperator(
        task_id='task_dbt_initialization',
        bash_command='cd /home/airflow && mkdir -p .dbt && \
            cp /opt/airflow/dags/.dbt/profiles.yml /home/airflow/.dbt/',
    )
    create_schema = PostgresOperator(
        task_id='task_create_schema_xkcd',
        postgres_conn_id='postgres',
        sql= """Create schema if not exists xkcd;""")
    create_table = PostgresOperator(
        task_id='create_table_comic',
        postgres_conn_id='postgres',
        sql= """CREATE TABLE if not exists xkcd.comic (
            alt TEXT,
            day TEXT NOT NULL,
            img TEXT,
            link TEXT,
            month TEXT NOT NULL,
            news TEXT,
            num INTEGER PRIMARY KEY,
            safe_title TEXT,
            title TEXT NOT NULL,
            transcript TEXT,
            year TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE OR REPLACE FUNCTION set_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.triggers
                    WHERE trigger_name = 'set_updated_at_trigger'
                    AND event_object_table = 'comic'
                ) THEN
                    CREATE TRIGGER set_updated_at_trigger
                    BEFORE UPDATE ON xkcd.comic
                    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
                END IF;
            END $$;
            """)
    dbt_run = BashOperator(
        task_id='task_dbt_run',
        bash_command='cd /opt/airflow/dags/dbt_xkcd && dbt run',
    )

    dbt_initialization >> create_schema >> create_table >> dbt_run