from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration


def downstream_function_branch_1():
    print("Upstream DAG 1 has completed. Starting tasks of branch 1.")


def downstream_function_branch_2():
    print("Upstream DAG 2 has completed. Starting tasks of branch 2.")


def downstream_function_branch_3():
    print("Upstream DAG 3 has completed. Starting tasks of branch 3.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=5),
}

with DAG(
    dag_id="dag_son",
    start_date=datetime(2022, 8, 1),
    max_active_runs=3,
    schedule="*/1 * * * *",
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    task_branch_1 = PythonOperator(
        task_id="task_branch_1",
        python_callable=downstream_function_branch_3,
    )

    task_branch_2 = PythonOperator(
        task_id="task_branch_2",
        python_callable=downstream_function_branch_3,
    )

    task_branch_3 = PythonOperator(
        task_id="task_branch_3",
        python_callable=downstream_function_branch_3,
    )

    start >> ets_branch_1 >> task_branch_1 >> ets_branch_3 >> end