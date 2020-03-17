import os.path
import csv
import sqlalchemy
import petl as etl
from collections import OrderedDict

from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient, BlobBlock
import azure.core.exceptions

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

_AIRFLOW_HOME=os.getenv('AIRFLOW_HOME')
_TEMP_FILE=f"{_AIRFLOW_HOME}/files"
_METADADOS=f"{_AIRFLOW_HOME}/metadado/list.csv"
_SOURCE='17'
_PROC_FILES=f"{_AIRFLOW_HOME}/tempfiles"



class ConnAzure2AzureOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 account_name,
                 account_key,
                 container_from,
                 container_to,
                 *args,
                 **kwargs) :
        super (ConnAzure2AzureOperator, self).__init__ (*args, **kwargs)
        self.client_from = ContainerClient (account_url = f"https://{account_name}.blob.core.windows.net/",
                                            credential = account_key, container_name = container_from)
        self.client_to = ContainerClient (account_url = f"https://{account_name}.blob.core.windows.net/",
                                          credential = account_key, container_name = container_to)
        self.container_from = container_from
        self.container_to = container_to


class OperatorCarregaVidas(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorCarregaVidas, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        engine = create_engine(
            "postgresql+psycopg2://giovani.stefani@prd-dictas-postgresql-server.postgres.database.azure.com:B7hqx9mFi19J8BTE@prd-dictas-postgresql-server.postgres.database.azure.com:5432/dictas")
        try:
            nm_arq='VIDAS.csv'
            nm_tbl='vidas'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            etl.todb(table, engine, f'{nm_tbl}')

            self.logger.info(f"tabela {nm_tbl} carregada")
            
        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')
'''
class OperatorAjustaBeneficiario(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaBeneficiario, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        engine = create_engine(
            "postgresql+psycopg2://giovani.stefani@prd-dictas-postgresql-server.postgres.database.azure.com:B7hqx9mFi19J8BTE@prd-dictas-postgresql-server.postgres.database.azure.com:5432/dictas")
        try:
            nm_arq='BENEFICIARIO.csv'
            nm_tbl='beneficiario'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            etl.todb(table1, engine, f'{nm_tbl}')

            self.logger.info(f"tabela {nm_tbl} carregada")
            
        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

class OperatorAjustaBeneficiario(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaBeneficiario, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        engine = create_engine(
            "postgresql+psycopg2://giovani.stefani@prd-dictas-postgresql-server.postgres.database.azure.com:B7hqx9mFi19J8BTE@prd-dictas-postgresql-server.postgres.database.azure.com:5432/dictas")
        try:
            nm_arq='BENEFICIARIO.csv'
            nm_tbl='beneficiario'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            etl.todb(table1, engine, f'{nm_tbl}')

            self.logger.info(f"tabela {nm_tbl} carregada")
            
        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)') 

class OperatorAjustaBeneficiario(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaBeneficiario, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        engine = create_engine(
            "postgresql+psycopg2://giovani.stefani@prd-dictas-postgresql-server.postgres.database.azure.com:B7hqx9mFi19J8BTE@prd-dictas-postgresql-server.postgres.database.azure.com:5432/dictas")
        try:
            nm_arq='BENEFICIARIO.csv'
            nm_tbl='beneficiario'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            etl.todb(table1, engine, f'{nm_tbl}')

            self.logger.info(f"tabela {nm_tbl} carregada")
            
        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)') 

class OperatorAjustaBeneficiario(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaBeneficiario, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        engine = create_engine(
            "postgresql+psycopg2://giovani.stefani@prd-dictas-postgresql-server.postgres.database.azure.com:B7hqx9mFi19J8BTE@prd-dictas-postgresql-server.postgres.database.azure.com:5432/dictas")
        try:
            nm_arq='BENEFICIARIO.csv'
            nm_tbl='beneficiario'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            etl.todb(table1, engine, f'{nm_tbl}')

            self.logger.info(f"tabela {nm_tbl} carregada")
            
        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

class OperatorAjustaBeneficiario(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaBeneficiario, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        engine = create_engine(
            "postgresql+psycopg2://giovani.stefani@prd-dictas-postgresql-server.postgres.database.azure.com:B7hqx9mFi19J8BTE@prd-dictas-postgresql-server.postgres.database.azure.com:5432/dictas")
        try:
            nm_arq='BENEFICIARIO.csv'
            nm_tbl='beneficiario'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            etl.todb(table1, engine, f'{nm_tbl}')

            self.logger.info(f"tabela {nm_tbl} carregada")
            
        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')   

class OperatorAjustaBeneficiario(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaBeneficiario, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        engine = create_engine(
            "postgresql+psycopg2://giovani.stefani@prd-dictas-postgresql-server.postgres.database.azure.com:B7hqx9mFi19J8BTE@prd-dictas-postgresql-server.postgres.database.azure.com:5432/dictas")
        try:
            nm_arq='BENEFICIARIO.csv'
            nm_tbl='beneficiario'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            etl.todb(table1, engine, f'{nm_tbl}')

            self.logger.info(f"tabela {nm_tbl} carregada")
            
        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')          

class OperatorAjustaBeneficiario(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaBeneficiario, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        engine = create_engine(
            "postgresql+psycopg2://giovani.stefani@prd-dictas-postgresql-server.postgres.database.azure.com:B7hqx9mFi19J8BTE@prd-dictas-postgresql-server.postgres.database.azure.com:5432/dictas")
        try:
            nm_arq='BENEFICIARIO.csv'
            nm_tbl='beneficiario'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            etl.todb(table1, engine, f'{nm_tbl}')

            self.logger.info(f"tabela {nm_tbl} carregada")
            
        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')                                                       
   
class OperatorAjustaBeneficiario(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaBeneficiario, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        engine = create_engine(
            "postgresql+psycopg2://giovani.stefani@prd-dictas-postgresql-server.postgres.database.azure.com:B7hqx9mFi19J8BTE@prd-dictas-postgresql-server.postgres.database.azure.com:5432/dictas")
        try:
            nm_arq='BENEFICIARIO.csv'
            nm_tbl='beneficiario'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            etl.todb(table1, engine, f'{nm_tbl}')

            self.logger.info(f"tabela {nm_tbl} carregada")
            
        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')
'''
class TransDadosPlugin(AirflowPlugin):
    name = "TransDadosPlugin"
    operators = [OperatorCarregaVidas]