import os
import airflow

from airflow.models import DAG
from datetime import timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import dates
from airflow.operators import (
    OperatorImport2Azure,
)

_AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
_TEMP_FILE = f'{_AIRFLOW_HOME}/tempfiles'
_SOURCE_FILES = '/usr/local/airflow/files'  # os.getenv('SOURCE_FILES')
_ACCOUNT_NAME = 'datalake2dictas'  # os.getenv('ACCOUNT_NAME')
_ACCOUNT_KEY = 'wlxJHcWvtVhPpL/zs6l+F1bJGKZnJ4HppZcVyh+ns32oH46E3dY/HBLau3V6um9hv+KZf/3mXEAL5nHD41X3jg=='

default_args = {
    'owner': 'giovani.stefani',
    'depends_on_past': False,
    'start_date': dates.days_ago(0),
    'email': 'giovani.stefani@outlook.com',
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

main_dag = DAG(
    'dag_import_arquivos',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    dagrun_timeout=timedelta(minutes=30),
    max_active_runs=1,
    catchup=False
)

start_dag = DummyOperator (
    task_id = 'start_dag' ,
    default_args = default_args ,
    dag = main_dag
)

end_dag = DummyOperator (
    task_id = 'end_dag',
    default_args = default_args,
    dag = main_dag,
    trigger_rule = 'all_done'
)

import_source2transient = OperatorImport2Azure (
    task_id='import_source2transient',
    default_args=default_args,
    account_name=_ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_name='transient',
    source=_SOURCE_FILES,
    provide_context=True,
    trigger_rule='all_done',
    previous_task='start_dag',
    num_workers=1, 
    dag=main_dag
)


start_dag >> import_source2transient
import_source2transient >> end_dag
