import os.path
import csv
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


class OperatorAjustaBeneficiario(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaBeneficiario, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='BENEFICIARIO.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            table1 = etl.convert(table, {'pk_beneficiario': str,
                             'fk_operadora': int
                            })
                
            etl.tocsv(table1, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

            self.logger.info(f"Destino do arquivo {self.client_to}")
            self.logger.info(f"Container destino {self.container_to}")

        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

        upload_file=f'{_PROC_FILES}/t{nm_arq}'

        try:
            if os.path.isfile(upload_file):
                with open (upload_file, "rb") as data :
                    self.client_to.upload_blob(nm_arq, data, overwrite = True)
                    self.logger.info(f'{data} carregado')
            else: 
                self.logger.info(f't{nm_arq} não foi encontrado no container')
        finally:
            self.logger.info('Tudo Carregado') 

class OperatorAjustaConsulta(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaConsulta, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='CONSULTAS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            table1 = etl.convert(table, {'fk_servico': int,
                                         'fk_operadora': int
                                        })
                
            etl.tocsv(table1, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

            self.logger.info(f"Destino do arquivo {self.client_to}")
            self.logger.info(f"Container destino {self.container_to}")

        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

        upload_file=f'{_PROC_FILES}/t{nm_arq}'

        try:
            if os.path.isfile(upload_file):
                with open (upload_file, "rb") as data :
                    self.client_to.upload_blob(nm_arq, data, overwrite = True)
                    self.logger.info(f'{data} carregado')
            else: 
                self.logger.info(f't{nm_arq} não foi encontrado no container')
        finally:
            self.logger.info('Tudo Carregado')  

class OperatorAjustaCustos(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaCustos, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='CUSTOS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            table1 = etl.convert(table, {'pk_custo': int,
                                         'fk_operadora': int
                                        })
                
            etl.tocsv(table1, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

            self.logger.info(f"Destino do arquivo {self.client_to}")
            self.logger.info(f"Container destino {self.container_to}")

        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

        upload_file=f'{_PROC_FILES}/t{nm_arq}'

        try:
            if os.path.isfile(upload_file):
                with open (upload_file, "rb") as data :
                    self.client_to.upload_blob(nm_arq, data, overwrite = True)
                    self.logger.info(f'{data} carregado')
            else: 
                self.logger.info(f't{nm_arq} não foi encontrado no container')
        finally:
            self.logger.info('Tudo Carregado')    

class OperatorAjustaDiarias(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaDiarias, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='DIARIAS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            table1 = etl.convert(table, {'pk_servico': int,
                                         'fk_operadora': int
                                        })
                
            etl.tocsv(table1, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

            self.logger.info(f"Destino do arquivo {self.client_to}")
            self.logger.info(f"Container destino {self.container_to}")

        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

        upload_file=f'{_PROC_FILES}/t{nm_arq}'

        try:
            if os.path.isfile(upload_file):
                with open (upload_file, "rb") as data :
                    self.client_to.upload_blob(nm_arq, data, overwrite = True)
                    self.logger.info(f'{data} carregado')
            else: 
                self.logger.info(f't{nm_arq} não foi encontrado no container')
        finally:
            self.logger.info('Tudo Carregado')  

class OperatorAjustaEmpresa(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaEmpresa, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='EMPRESA.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            table1 = etl.convert(table, {'pk_empresa': int,
                                         'fk_operadora': int
                                        })
                
            etl.tocsv(table1, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

            self.logger.info(f"Destino do arquivo {self.client_to}")
            self.logger.info(f"Container destino {self.container_to}")

        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

        upload_file=f'{_PROC_FILES}/t{nm_arq}'

        try:
            if os.path.isfile(upload_file):
                with open (upload_file, "rb") as data :
                    self.client_to.upload_blob(nm_arq, data, overwrite = True)
                    self.logger.info(f'{data} carregado')
            else: 
                self.logger.info(f't{nm_arq} não foi encontrado no container')
        finally:
            self.logger.info('Tudo Carregado')      

class OperatorAjustaPrestadores(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaPrestadores, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='PRESTADORES.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            table1 = etl.convert(table, {'pk_medico_prestador': int,
                                         'fk_operadora': int
                                        })
                
            etl.tocsv(table1, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

            self.logger.info(f"Destino do arquivo {self.client_to}")
            self.logger.info(f"Container destino {self.container_to}")

        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

        upload_file=f'{_PROC_FILES}/t{nm_arq}'

        try:
            if os.path.isfile(upload_file):
                with open (upload_file, "rb") as data :
                    self.client_to.upload_blob(nm_arq, data, overwrite = True)
                    self.logger.info(f'{data} carregado')
            else: 
                self.logger.info(f't{nm_arq} não foi encontrado no container')
        finally:
            self.logger.info('Tudo Carregado')           

class OperatorAjustaReceitas(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaReceitas, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='RECEITAS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            table1 = etl.convert(table, {'fk_operadora': int,
                                         'vl_cobranca' : float,  
                                         'vl_pago' : float
                                        })
                
            etl.tocsv(table1, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

            self.logger.info(f"Destino do arquivo {self.client_to}")
            self.logger.info(f"Container destino {self.container_to}")

        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

        upload_file=f'{_PROC_FILES}/t{nm_arq}'

        try:
            if os.path.isfile(upload_file):
                with open (upload_file, "rb") as data :
                    self.client_to.upload_blob(nm_arq, data, overwrite = True)
                    self.logger.info(f'{data} carregado')
            else: 
                self.logger.info(f't{nm_arq} não foi encontrado no container')
        finally:
            self.logger.info('Tudo Carregado')           

class OperatorAjustaServico(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaServico, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='SERVICOS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            table1 = etl.convert(table, {'fk_operadora': int,
                                         'pk_servico' : int ,
                                         'tipo' : str
                                        })
                
            etl.tocsv(table1, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

            self.logger.info(f"Destino do arquivo {self.client_to}")
            self.logger.info(f"Container destino {self.container_to}")

        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

        upload_file=f'{_PROC_FILES}/t{nm_arq}'

        try:
            if os.path.isfile(upload_file):
                with open (upload_file, "rb") as data :
                    self.client_to.upload_blob(nm_arq, data, overwrite = True)
                    self.logger.info(f'{data} carregado')
            else: 
                self.logger.info(f't{nm_arq} não foi encontrado no container')
        finally:
            self.logger.info('Tudo Carregado')                                                         
   
class OperatorAjustaVidas(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorAjustaVidas, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='VIDAS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            table1 = etl.convert(table, {'fk_operadora': int,
                                         'fk_beneficiario' : str
                                        })
                
            etl.tocsv(table1, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

            self.logger.info(f"Destino do arquivo {self.client_to}")
            self.logger.info(f"Container destino {self.container_to}")

        except azure.core.exceptions.ResourceNotFoundError:
            print('Entrou na exceção :)')

        upload_file=f'{_PROC_FILES}/t{nm_arq}'

        try:
            if os.path.isfile(upload_file):
                with open (upload_file, "rb") as data :
                    self.client_to.upload_blob(nm_arq, data, overwrite = True)
                    self.logger.info(f'{data} carregado')
            else: 
                self.logger.info(f't{nm_arq} não foi encontrado no container')
        finally:
            self.logger.info('Tudo Carregado') 

class TransDadosPlugin(AirflowPlugin):
    name = "TransDadosPlugin"
    operators = [OperatorAjustaBeneficiario, OperatorAjustaConsulta, OperatorAjustaCustos, OperatorAjustaDiarias, OperatorAjustaEmpresa, OperatorAjustaPrestadores, OperatorAjustaReceitas, OperatorAjustaServico, OperatorAjustaVidas]