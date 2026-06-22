from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from jobs.run_real_estate_pipeline import run_pipeline

with DAG(
    dag_id="real_estate_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        op_kwargs={
            "deal_ymd": "{{ data_interval_start.subtract(months=1).strftime('%Y%m') }}"
        }
    )