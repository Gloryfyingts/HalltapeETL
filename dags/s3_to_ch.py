import os
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.db import provide_session
# from airflow.models import DagRun
# from airflow.sensors.external_task import ExternalTaskSensor
from airflow.exceptions import AirflowSkipException, AirflowSensorTimeout
from airflow.models import Variable
import boto3 

default_args = {'owner':'owner', 'retries':3}


s3_to_ch = DAG(
    dag_id = "s3_to_ch_dag",
    start_date = pendulum.datetime(2025, 7, 23, 0, 0, 0),
    schedule="15 12 * * *",
    max_active_tasks=2,
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    doc_md=__doc__,
)

with s3_to_ch as dag:

    def check_s3_file(**context):
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        )
        bucket= "prod"
        ds = context['ds']
        key = f"api/earthquake/events_{ds}.json"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=key)
        if not response:
            raise AirflowSkipException("Нет новых файлов.")

        return "Contents" in response and len(response["Contents"]) > 0

    check_s3_sensor = PythonSensor(
        task_id="check_s3_file",
        python_callable=check_s3_file,
        mode="reschedule",
        poke_interval=60,
        timeout=60*60,
    )


    s3_to_ch_full = SparkSubmitOperator(
    task_id='spark_s3_to_ch',
    application='/opt/airflow/scripts/transform/earthquake_s3_to_ch.py',
    conn_id='spark_default',
    env_vars={
        'CLICKHOUSE_JDBC_URL': 'jdbc:clickhouse://clickhouse:8124/default',
        'CLICKHOUSE_USER': os.getenv('CLICKHOUSE_USER'),
        'CLICKHOUSE_PASSWORD': os.getenv('CLICKHOUSE_PASSWORD'),
        'S3_PATH_EARTHQUAKE': f's3a://prod/api/earthquake/',
        'PYTHONPATH': '/opt/airflow/plugins:/opt/airflow/scripts'
    },
    conf={
        # "spark.executor.instances": "1",
        # "spark.executor.memory": "2g",
        # "spark.executor.cores": "1",
        # "spark.driver.memory": "1g",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
    },
    packages=(
        "org.apache.hadoop:hadoop-aws:3.4.0,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"
    ),
    application_args=[
        "--process-date", "{{ ds }}",
        "--table-raw", "earth_quake_full",
        "--table-agg", "earth_quake_agg"
        ],
    dag=dag
    )