from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=5),
}

def print_start(**kwargs):
    print("Starting the workflow.")

def decide_branch_son1(**kwargs):
    return "son2"

def decide_branch_son2(**kwargs):
    return "son3"

def print_terminate(**kwargs):
    print("Workflow terminated.")
    return "terminate_workflow"

with DAG(
    dag_id="father_dag",
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule_interval=None,  # Disable automatic scheduling
    default_args=default_args,
    catchup=False,
) as father_dag:

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=print_start,
    )

    branch_son1_task = BranchPythonOperator(
        task_id="branch_son1_task",
        python_callable=decide_branch_son1,
    )

    branch_son2_task = BranchPythonOperator(
        task_id="branch_son2_task",
        python_callable=decide_branch_son2,
    )

    terminate_workflow = PythonOperator(
        task_id="terminate_workflow",
        python_callable=print_terminate,
    )

    son1_trigger = TriggerDagRunOperator(
        task_id="son1_trigger",
        trigger_dag_id="son1",
        dag=father_dag,
    )

    son2_trigger = TriggerDagRunOperator(
        task_id="son2_trigger",
        trigger_dag_id="son2",
        dag=father_dag,
    )

    son3_trigger = TriggerDagRunOperator(
        task_id="son3_trigger",
        trigger_dag_id="son3",
        dag=father_dag,
    )

    # Set up the order of tasks in the father DAG
    start_task >> son1_trigger >> branch_son2_task
    branch_son2_task >> [son2_trigger, terminate_workflow]

    son2_trigger >> son3_trigger
    son3_trigger >> terminate_workflow



    # Set up the order of tasks in the father DAG
    # start_task >> branch_son1_task
    # branch_son1_task >> [son1_trigger, terminate_workflow]

    # son1_trigger >> branch_son2_task
    # branch_son2_task >> [son2_trigger, terminate_workflow]

    # son2_trigger >> son3_trigger
    # son3_trigger >> terminate_workflow

