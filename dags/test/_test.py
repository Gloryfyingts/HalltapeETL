from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="test_simple", 
    start_date=datetime(2025,1,1), 
    schedule=None,
    tags=["test_del"]
):
    EmptyOperator(task_id="test")
