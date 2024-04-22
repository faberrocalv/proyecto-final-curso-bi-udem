import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from supermercado_etl import ejecutar_etl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 4, 21),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1)
}

dag = DAG(
    'supermercado_dag',
    default_args=default_args,
    description='Supermercado ETL',
    schedule_interval=datetime.timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='supermercado_etl',
    python_callable=ejecutar_etl,
    dag=dag,
)

run_etl