from airflow import DAG
from airflow.operators.python import PythonOperator
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

# Define the son1 DAG
with DAG(
    dag_id="son1",
    start_date=datetime(2023, 1, 1),
    default_args=default_args,
    catchup=False,
) as son1_dag:

    def son1_task(**kwargs):
        print("Son1 DAG has been executed successfully.")

    son1_task = PythonOperator(
        task_id="son1_task",
        python_callable=son1_task,
    )

# Define the son2 DAG
with DAG(
    dag_id="son2",
    start_date=datetime(2023, 1, 1),
    default_args=default_args,
    catchup=False,
) as son2_dag:

    def son2_task(**kwargs):
        print("Son2 DAG has been executed successfully.")

    son2_task = PythonOperator(
        task_id="son2_task",
        python_callable=son2_task,
    )

# Define the son3 DAG
with DAG(
    dag_id="son3",
    start_date=datetime(2023, 1, 1),
    default_args=default_args,
    catchup=False,
) as son3_dag:

    def son3_task(**kwargs):
        print("Son3 DAG has been executed successfully.")

    son3_task = PythonOperator(
        task_id="son3_task",
        python_callable=son3_task,
    )

# Create the father DAG
with DAG(
    dag_id="father_dagaaa",
    start_date=datetime(2023, 1, 1),
    default_args=default_args,
    catchup=False,
) as father_dag:

    def print_start(**kwargs):
        print("Starting the workflow.")

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=print_start,
    )

    def always_true(**kwargs):
        return "son1_task"

    branch_task_son1 = PythonOperator(
        task_id="branch_task_son1",
        python_callable=always_true,
    )

    def always_true(**kwargs):
        return "son2_task"

    branch_task_son2 = PythonOperator(
        task_id="branch_task_son2",
        python_callable=always_true,
    )

    def print_terminate(**kwargs):
        print("Workflow terminated.")
        return "terminate_workflow"

    terminate_workflow = PythonOperator(
        task_id="terminate_workflow",
        python_callable=print_terminate,
    )

    # Set up the order of tasks in the father DAG
    start_task >> branch_task_son1
    branch_task_son1 >> son1_dag
    branch_task_son1 >> terminate_workflow

    son1_dag >> branch_task_son2
    branch_task_son2 >> son2_dag
    branch_task_son2 >> terminate_workflow

    son2_dag >> son3_dag
    son3_dag >> terminate_workflow
