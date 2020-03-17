import os

from airflow.models import DAG
from datetime import  timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import dates
from airflow.operators import (
    OperatorTransBeneficiario,
    OperatorTransConsulta,
    OperatorTransCusto,
    OperatorTransDiarias,
    OperatorTransEmpresas,
    OperatorTransPrestadores,
    OperatorTransReceitas,
    OperatorTransServicos,
    OperatorTransVidas,
    OperatorAjustaBeneficiario,
    OperatorAjustaConsulta,
    OperatorAjustaCustos, 
    OperatorAjustaDiarias, 
    OperatorAjustaEmpresa, 
    OperatorAjustaPrestadores, 
    OperatorAjustaReceitas, 
    OperatorAjustaServico, 
    OperatorAjustaVidas
)

_AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
_TEMP_FILE = f'{_AIRFLOW_HOME}/tempfiles'
_SOURCE_FILES = '/usr/local/airflow/files'  # os.getenv('SOURCE_FILES')
_ACCOUNT_NAME = 'datalake2dictas'  # os.getenv('ACCOUNT_NAME')
_ACCOUNT_KEY  = 'wlxJHcWvtVhPpL/zs6l+F1bJGKZnJ4HppZcVyh+ns32oH46E3dY/HBLau3V6um9hv+KZf/3mXEAL5nHD41X3jg=='

default_args = {
    'owner': 'giovani.stefani',
    'depends_on_past': False,
    'start_date': dates.days_ago(0),
    'email' : 'dictas@softplan.com.br' ,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

main_dag = DAG (
    'dag_tran_dados' ,
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

agg_dag = DummyOperator (
    task_id = 'agg_dag' ,
    default_args = default_args ,
    dag = main_dag
)

end_dag = DummyOperator (
    task_id = 'end_dag',
    default_args = default_args,
    dag = main_dag,
    trigger_rule = 'all_done'
)

trans_beneficiario = OperatorTransBeneficiario (
    task_id = 'trans_beneficiario' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='transient',
    container_to='raw',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='start_dag',
    num_workers=1,
    dag = main_dag
)

trans_consulta = OperatorTransConsulta (
    task_id = 'trans_consulta' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='transient',
    container_to='raw',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='start_dag',
    num_workers=1,
    dag = main_dag
)

trans_custos = OperatorTransCusto (
    task_id = 'trans_custos' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='transient',
    container_to='raw',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='start_dag',
    num_workers=1,
    dag = main_dag
)

trans_diarias = OperatorTransDiarias (
    task_id = 'trans_diarias' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='transient',
    container_to='raw',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='start_dag',
    num_workers=1,
    dag = main_dag
)

trans_empresas = OperatorTransEmpresas (
    task_id = 'trans_empresas' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='transient',
    container_to='raw',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='start_dag',
    num_workers=1,
    dag = main_dag
)

trans_prestadores = OperatorTransPrestadores (
    task_id = 'trans_prestadores' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='transient',
    container_to='raw',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='start_dag',
    num_workers=1,
    dag = main_dag
)

trans_receitas = OperatorTransReceitas (
    task_id = 'trans_receitas' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='transient',
    container_to='raw',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='start_dag',
    num_workers=1,
    dag = main_dag
)

trans_servicos = OperatorTransServicos (
    task_id = 'trans_servicos' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='transient',
    container_to='raw',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='start_dag',
    num_workers=1,
    dag = main_dag
)

trans_vidas = OperatorTransVidas (
    task_id = 'trans_vidas' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='transient',
    container_to='raw',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='start_dag',
    num_workers=1,
    dag = main_dag
)

ajusta_beneficiario = OperatorAjustaBeneficiario (
    task_id = 'ajusta_beneficiario' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='raw',
    container_to='refined',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='agg_dag',
    num_workers=1,
    dag = main_dag
)

ajusta_consulta = OperatorAjustaConsulta (
    task_id = 'ajusta_consulta' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='raw',
    container_to='refined',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='agg_dag',
    num_workers=1,
    dag = main_dag
)

ajusta_custo = OperatorAjustaCustos (
    task_id = 'ajusta_custo' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='raw',
    container_to='refined',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='agg_dag',
    num_workers=1,
    dag = main_dag
)

ajusta_diarias = OperatorAjustaDiarias (
    task_id = 'ajusta_diarias' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='raw',
    container_to='refined',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='agg_dag',
    num_workers=1,
    dag = main_dag
)

ajusta_empresas = OperatorAjustaEmpresa (
    task_id = 'ajusta_empresas' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='raw',
    container_to='refined',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='agg_dag',
    num_workers=1,
    dag = main_dag
)

ajusta_prestadores = OperatorAjustaPrestadores (
    task_id = 'ajusta_prestadores' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='raw',
    container_to='refined',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='agg_dag',
    num_workers=1,
    dag = main_dag
)

ajusta_receitas = OperatorAjustaReceitas (
    task_id = 'ajusta_receitas' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='raw',
    container_to='refined',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='agg_dag',
    num_workers=1,
    dag = main_dag
)

ajusta_servico = OperatorAjustaServico (
    task_id = 'ajusta_servico' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='raw',
    container_to='refined',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='agg_dag',
    num_workers=1,
    dag = main_dag
)

ajusta_vidas = OperatorAjustaVidas (
    task_id = 'ajusta_vidas' ,
    default_args = default_args ,
    account_name= _ACCOUNT_NAME,
    account_key=_ACCOUNT_KEY,
    container_from='raw',
    container_to='refined',
    source=_SOURCE_FILES,
    provide_context=True,
    previous_task='agg_dag',
    num_workers=1,
    dag = main_dag
)


start_dag >> [trans_beneficiario, trans_consulta, trans_custos, trans_diarias, trans_empresas, trans_prestadores, trans_receitas, trans_servicos, trans_vidas] >> agg_dag
agg_dag >> [ajusta_beneficiario, ajusta_consulta, ajusta_custo, ajusta_diarias, ajusta_empresas, ajusta_prestadores, ajusta_receitas, ajusta_servico, ajusta_vidas]>>end_dag
