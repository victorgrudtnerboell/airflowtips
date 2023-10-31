from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration

def print_task_type(**kwargs):
    print(f"The {kwargs['task_type']} task has completed.")

def decide_branch(**kwargs):
    # Determine the branch based on the result of the first TriggerDagRunOperator
    if kwargs['ti'].xcom_pull(task_ids="trigger_dependent_dag") == "success":
        return "end_task"
    else:
        return "trigger_dependent_dag2"

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=5),
}

with DAG(
    dag_id="dag_branch_dependency",
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    start_task = PythonOperator(
        task_id="start_task",
        python_callable=print_task_type,
        op_kwargs={"task_type": "starting"},
    )

    trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="dag_son",
        wait_for_completion=True
    )

    end_task = PythonOperator(
        task_id="end_task",
        python_callable=print_task_type,
        op_kwargs={"task_type": "ending"},
    )

    trigger_dependent_dag2 = TriggerDagRunOperator(
        task_id="trigger_dependent_dag2",
        trigger_dag_id="dag_son",
        wait_for_completion=True
    )

    decide_branch_task = BranchPythonOperator(
        task_id="decide_branch",
        python_callable=decide_branch,
    )

    start_task >> trigger_dependent_dag >> decide_branch_task
    decide_branch_task >> [end_task, trigger_dependent_dag2]
