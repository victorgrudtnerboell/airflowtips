from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration

def print_task_type(**kwargs):
    print(f"The {kwargs['task_type']} task has completed.")

def decide_branch(**kwargs):
    # Determine the branch based on some condition
    # You can modify this logic as needed
    if some_condition:  # Replace with your condition
        return "trigger_son1"
    elif some_other_condition:  # Replace with another condition
        return "trigger_son2"
    else:
        return "trigger_son3"

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
    dag_id="dag_father",
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule_interval=None,  # Disable automatic scheduling
    default_args=default_args,
    catchup=False,
) as dag:
    start_task = PythonOperator(
        task_id="start_task",
        python_callable=print_task_type,
        op_kwargs={"task_type": "starting"},
    )

    decide_branch_task = BranchPythonOperator(
        task_id="decide_branch",
        python_callable=decide_branch,
    )

    trigger_son1 = TriggerDagRunOperator(
        task_id="trigger_son1",
        trigger_dag_id="dag_son1",
        dag=dag,  # Assign the parent DAG
    )

    trigger_son2 = TriggerDagRunOperator(
        task_id="trigger_son2",
        trigger_dag_id="dag_son2",
        dag=dag,  # Assign the parent DAG
    )

    trigger_son3 = TriggerDagRunOperator(
        task_id="trigger_son3",
        trigger_dag_id="dag_son3",
        dag=dag,  # Assign the parent DAG
    )

    start_task >> decide_branch_task
    decide_branch_task >> [trigger_son1, trigger_son2, trigger_son3]
