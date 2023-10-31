from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration

def print_task_type(**kwargs):
    print(f"The {kwargs['task_type']} task has completed.")

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=5),
}

def create_branching_dag(dag_id, condition_task, trigger_son_task, finish_task):
    with DAG(
        dag_id=dag_id,
        start_date=datetime(2023, 1, 1),
        max_active_runs=1,
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
    ) as dag:
        start_task = PythonOperator(
            task_id=f"{dag_id}_start_task",
            python_callable=print_task_type,
            op_kwargs={"task_type": "starting"},
        )

        trigger_son = TriggerDagRunOperator(
            task_id=f"{dag_id}_trigger_son",
            trigger_dag_id="dag_son",
            wait_for_completion=True
        )

        finish_task = PythonOperator(
            task_id=f"{dag_id}_finish_task",
            python_callable=print_task_type,
            op_kwargs={"task_type": "ending"},
        )

        start_task >> condition_task
        condition_task >> [trigger_son, finish_task]

    return dag

# Define your condition, trigger, and finish tasks for each DAG
condition_task_1 = YourConditionTask1()
trigger_son_task_1 = YourTriggerSonTask1()
finish_task_1 = YourFinishTask1()

condition_task_2 = YourConditionTask2()
trigger_son_task_2 = YourTriggerSonTask2()
finish_task_2 = YourFinishTask2()

condition_task_3 = YourConditionTask3()
trigger_son_task_3 = YourTriggerSonTask3()
finish_task_3 = YourFinishTask3()

# Create three branching DAGs
dag_1 = create_branching_dag("dag_father_1", condition_task_1, trigger_son_task_1, finish_task_1)
dag_2 = create_branching_dag("dag_father_2", condition_task_2, trigger_son_task_2, finish_task_2)
dag_3 = create_branching_dag("dag_father_3", condition_task_3, trigger_son_task_3, finish_task_3)
