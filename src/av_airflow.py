from airflow import DAG
from airflow.operators.python import PythonOperator
from av_update import run
from datetime import datetime

with DAG(
    'stock_market_dag', 
    start_date=datetime(2024, 1, 1), 
    schedule_interval='10 0 * * *',
    catchup=False
):
    etl_update = PythonOperator(
        task_id='etl_update',
        python_callable=run,
    )