import pendulum
import textwrap

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "depends_on_past": False,
    "retries": 5,
    "rerty_delay": pendulum.duration(minutes=1),
}


with DAG(
    dag_id="_example",
    default_args=default_args,
    schedule=pendulum.duration(minutes=1),
    start_date=pendulum.now() - pendulum.duration(minutes=5),
    catchup=False,
    tags=["test"],
    description="Test example DAG, didn't use."
) as dag:
    """
    ###DAG documentation.
    Documentation.
    """
    dag.doc_md = __doc__
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )
    t1.doc_md = textwrap.dedent(
        """
    ###Task documentation.
    Documentation.
    """
    )
    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
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
