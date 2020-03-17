import os

from airflow.models import DAG
from datetime import  timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import dates
from airflow.operators import (
    LoadAzure2AzureOperator,
    CompareAzure2AzureOperator,
    DeleteFilesAzure2AzureOperator,
    LoadLocal2AzureOperator,
    CompareLocal2AzureOperator,
    ConsolidateServicoOperator,
    CompareAzure2PostgresOperator,
    LoadAzure2PostgresOperator
)

_AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
_TEMP_FILE = f'{_AIRFLOW_HOME}/tempfiles'
_SOURCE_FILES = '/usr/local/airflow/files'  # os.getenv('SOURCE_FILES')
_ACCOUNT_NAME = 'datalake2dictas'  # os.getenv('ACCOUNT_NAME')
_ACCOUNT_KEY  = 'wlxJHcWvtVhPpL/zs6l+F1bJGKZnJ4HppZcVyh+ns32oH46E3dY/HBLau3V6um9hv+KZf/3mXEAL5nHD41X3jg=='
_LISTFILESSERVICO=['consultas/CONSULTAS.pkl','diarias/DIARIAS.pkl','servico/SERVICOS.pkl']

"""
# DB DEV

_USERDB = 'airflow'
_PASSDB='airflow'
_LINKDB='postgres'
_PORTDB='5432'
_DB='dictas'
"""
# DB PROD

_USERDB = 'etl@prd-dictas-postgresql-server.postgres.database.azure.com'
_PASSDB='8IUGjRyaNKQIlesq'
_LINKDB='prd-dictas-postgresql-server.postgres.database.azure.com'
_PORTDB='5432'
_DB='dictas'

default_args = {
    'owner' : 'marcio.panucci' ,
    'depends_on_past' : False ,
    'start_date' : dates.days_ago(0) ,
    'email' : 'dictas@softplan.com.br' ,
    'email_on_failure' : False ,
    'retries' : 1 ,
    'retry_delay' : timedelta ( minutes = 1 )
}

main_dag = DAG (
    'dag_dictas' ,
    default_args = default_args ,
    schedule_interval = '*/10 * * * *' ,
    dagrun_timeout = timedelta ( hours = 10 ) ,
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

diff_transient2raw = CompareAzure2AzureOperator (
    task_id = 'diff_transient2raw' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='transient',
    container_to='raw',
    extension_from='csv',
    extension_to='pkl',
    provide_context=True,
    num_workers=3,
    dag = main_dag
)

run_raw_worker_1 = LoadAzure2AzureOperator (
        task_id = 'run_raw_worker_1',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_from='transient',
        container_to = 'raw',
        sep='|',
        previous_task='diff_transient2raw',
        worker='worker_1',
        processor='transient2raw',
        extension_from='csv',
        extension_to='pkl',
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )

run_raw_worker_2 = LoadAzure2AzureOperator (
        task_id = 'run_raw_worker_2',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_from='transient',
        container_to = 'raw',
        sep='|',
        previous_task='diff_transient2raw',
        worker='worker_2',
        processor='transient2raw',
        extension_from='csv',
        extension_to='pkl',
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )


run_raw_worker_3 = LoadAzure2AzureOperator (
        task_id = 'run_raw_worker_3',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_from='transient',
        container_to = 'raw',
        sep='|',
        previous_task='diff_transient2raw',
        worker='worker_3',
        processor='transient2raw',
        extension_from='csv',
        extension_to='pkl',
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )


delete_transient = DeleteFilesAzure2AzureOperator (
    task_id = 'delete_transient' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container='transient',
    provide_context=True,
    dag = main_dag
)

diff_raw2trusted = CompareAzure2AzureOperator (
    task_id = 'diff_raw2trusted' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='raw',
    container_to='trusted',
    extension_from='pkl',
    extension_to='pkl',
    provide_context=True,
    num_workers=3,
    dag = main_dag
)

run_trusted_worker_1 = LoadAzure2AzureOperator (
        task_id = 'run_trusted_worker_1',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_from='raw',
        container_to = 'trusted',
        sep='|',
        previous_task='diff_raw2trusted',
        worker='worker_1',
        processor='raw2trusted',
        extension_from='pkl',
        extension_to='pkl',
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )

run_trusted_worker_2 = LoadAzure2AzureOperator (
        task_id = 'run_trusted_worker_2',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_from='raw',
        container_to = 'trusted',
        sep='|',
        previous_task='diff_raw2trusted',
        worker='worker_2',
        processor='raw2trusted',
        extension_from='pkl',
        extension_to='pkl',
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )

run_trusted_worker_3 = LoadAzure2AzureOperator (
        task_id = 'run_trusted_worker_3',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_from='raw',
        container_to = 'trusted',
        sep='|',
        previous_task='diff_raw2trusted',
        worker='worker_3',
        processor='raw2trusted',
        extension_from='pkl',
        extension_to='pkl',
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )


diff_trusted2refined = CompareAzure2AzureOperator (
    task_id = 'diff_trusted2refined' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='trusted',
    container_to='refined',
    extension_from='pkl',
    extension_to='pkl',
    provide_context=True,
    num_workers=3,
    dag = main_dag
)

run_refined_worker_1 = LoadAzure2AzureOperator (
        task_id = 'run_refined_worker_1',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_from='trusted',
        container_to = 'refined',
        sep='|',
        previous_task='diff_trusted2refined',
        worker='worker_1',
        processor='trusted2refined',
        extension_from='pkl',
        extension_to='pkl',
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )

run_refined_worker_2 = LoadAzure2AzureOperator (
        task_id = 'run_refined_worker_2',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_from='trusted',
        container_to = 'refined',
        sep='|',
        previous_task='diff_trusted2refined',
        worker='worker_2',
        processor='trusted2refined',
        extension_from='pkl',
        extension_to='pkl',
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )

run_refined_worker_3 = LoadAzure2AzureOperator (
        task_id = 'run_refined_worker_3',
        account_name=_ACCOUNT_NAME,
        account_key=_ACCOUNT_KEY,
        container_from='trusted',
        container_to = 'refined',
        sep='|',
        previous_task='diff_trusted2refined',
        worker='worker_3',
        processor='trusted2refined',
        extension_from='pkl',
        extension_to='pkl',
        default_args = default_args,
        dag = main_dag,
        trigger_rule = 'all_done'
    )

consolidate_servico = ConsolidateServicoOperator (
    task_id = 'consolidate_servico',
    account_name=_ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container='refined',
    listfiles=_LISTFILESSERVICO,
    dir_out='servico',
    nm_out='SERVICO_CONSOLIDADO',
    extension='pkl',
    previous_task='diff_trusted2refined',
    dag = main_dag,
    trigger_rule = 'all_done'
)

diff_refined2postgres = CompareAzure2PostgresOperator (
    task_id = 'diff_refined2postgres' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container='refined',
    user=_USERDB,
    password=_PASSDB,
    link=_LINKDB,
    port=_PORTDB,
    db=_DB,
    dag = main_dag
)

load_postgres = LoadAzure2PostgresOperator (
    task_id = 'load_postgres',
    account_name=_ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container='refined',
    user=_USERDB,
    password=_PASSDB,
    link=_LINKDB,
    port=_PORTDB,
    db=_DB,
    previous_task='diff_refined2postgres',
    extension='pkl',
    chunksize=10000,
    dag = main_dag,
    trigger_rule = 'all_done'
)


start_dag >> diff_transient2raw
diff_transient2raw >> [run_raw_worker_1, run_raw_worker_2, run_raw_worker_3] >> delete_transient
delete_transient >> diff_raw2trusted
diff_raw2trusted >> [run_trusted_worker_1, run_trusted_worker_2, run_trusted_worker_3] >> diff_trusted2refined
diff_trusted2refined >> [run_refined_worker_1, run_refined_worker_2, run_refined_worker_3] >> consolidate_servico
consolidate_servico >> diff_refined2postgres
diff_refined2postgres >> load_postgres >> end_dag
