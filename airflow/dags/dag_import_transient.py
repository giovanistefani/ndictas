import os

from airflow.models import DAG
from datetime import  timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import dates
from airflow.operators import (
    LoadAzure2AzureOperator,
    CompareAzure2AzureOperator,
    LoadLocal2AzureOperator,
    CompareLocal2AzureOperator,
)

_AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
_TEMP_FILE = f'{_AIRFLOW_HOME}/tempfiles'
_SOURCE_FILES = '/usr/local/airflow/files'  # os.getenv('SOURCE_FILES')
_ACCOUNT_NAME = 'datalake2dictas'  # os.getenv('ACCOUNT_NAME')
_ACCOUNT_KEY  = 'wlxJHcWvtVhPpL/zs6l+F1bJGKZnJ4HppZcVyh+ns32oH46E3dY/HBLau3V6um9hv+KZf/3mXEAL5nHD41X3jg=='

default_args = {
    'owner' : 'marcio.panucci' ,
    'depends_on_past' : False ,
    'start_date' : dates.days_ago(0) ,
    'email' : 'dictas@softplan.com.br' ,
    'email_on_failure' : False ,
    'retries' : 1 ,
    'retry_delay' : timedelta ( minutes = 2 )
}

main_dag = DAG (
    'dag_import_transient' ,
    default_args = default_args ,
    schedule_interval = '*/10 * * * *' ,
    dagrun_timeout = timedelta ( minutes = 60 ) ,
    max_active_runs = 1 ,
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

diff_source2transient = CompareLocal2AzureOperator (
    task_id = 'diff_source2transient' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_name='transient',
    source=_SOURCE_FILES,
    provide_context=True,
    num_workers=3,
    dag = main_dag
)

run_transient_worker_1 = LoadLocal2AzureOperator (
        task_id = 'run_transient_worker_1',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_name='transient',
        source=_SOURCE_FILES,
        previous_task='diff_source2transient',
        worker='worker_1',
        provide_context=True,
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )

run_transient_worker_2 = LoadLocal2AzureOperator (
        task_id = 'run_transient_worker_2',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_name='transient',
        source=_SOURCE_FILES,
        previous_task='diff_source2transient',
        worker='worker_2',
        provide_context=True,
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )

run_transient_worker_3 = LoadLocal2AzureOperator (
        task_id = 'run_transient_worker_3',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_name='transient',
        source=_SOURCE_FILES,
        previous_task='diff_source2transient',
        worker='worker_3',
        provide_context=True,
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )


start_dag >> diff_source2transient
diff_source2transient >> [run_transient_worker_1, run_transient_worker_2, run_transient_worker_3]  >>  end_dag
