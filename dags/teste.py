from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
from datetime import datetime, timedelta
from pytz import timezone


default_args = {
  'owner': 'SALESFORCE-INGESTION',
  'depends_on_past': False,
  'retries': 3,
  'retry_delay': timedelta(minutes=5)
}

# declare dag
dag = DAG(
    'Teste',
    default_args=default_args,
    start_date= datetime(2023, 8, 25, 6, 0, tzinfo=timezone('America/Sao_Paulo')),
    schedule_interval= None,#'0 6-22/2 * * *',
    tags=['Kafka', 'bronze','senior'],
    catchup=False,
    max_active_runs=1
)

extract_transform_task = PythonOperator(
    task_id='extract_transform_data',
    python_callable=envia_kafka_topico,
    provide_context=True,
    dag=dag
)

extract_transform_task