import textwrap

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "Generate_initial_data__csv",
    default_args=default_args,
    description="Generate initial csv data set in data/csv directory.",
    schedule=timedelta(minutes=1),
    start_date=datetime(2025, 12, 6),
    catchup=False,
    tags=["source"],
) as dag:
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )
    t2 = BashOperator(
        task_id = "do_sleep",
        bash_command="sleep 5",
        depends_on_past=False,
        retries=3,
    )
    templated_command = textwrap.dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7) }}"
    {% endfor %}
    """
    )
    t3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1 >> [t2, t3] # type: ignore
