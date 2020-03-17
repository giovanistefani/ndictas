import os.path
import csv
import petl as etl

from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient, BlobBlock

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

_AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
_TEMP_FILE = f"{_AIRFLOW_HOME}/files"
_METADADOS = f"{_AIRFLOW_HOME}/metadado/list.csv"
_SOURCE = '17'


class ConnLocal2AzureOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 account_name,
                 account_key,
                 container_name,
                 source,
                 *args,
                 **kwargs):
        super(ConnLocal2AzureOperator, self).__init__(*args, **kwargs)
        self.client = ContainerClient(account_url=f"https://{account_name}.blob.core.windows.net/",
                                      credential=account_key, container_name=container_name)
        self.source = source


class OperatorImport2Azure(ConnLocal2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorImport2Azure, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            with open(_METADADOS, newline='') as lista:
                reader = csv.DictReader(lista)
                for nm_file in reader:
                    file = nm_file['ARQUIVOS']
                    self.logger.info(f'{file} aqui')
                    blob = self.client.get_blob_client(file)
                    upload_file = f'{_TEMP_FILE}/{file}'
                    t_arq = os.path.isfile(upload_file)
                    if t_arq == True:
                        with open(upload_file, "rb") as data:
                            blob.upload_blob(data, overwrite=True)
                            self.logger.info(f'{file} carregado')
                else:
                    self.logger.info(f'{file} não encontrado')
        finally:
            self.logger.info(f'Todos os arquivos carregados')


class Import2AzurePlugin(AirflowPlugin):
    name = "Import2Azure"
    operators = [OperatorImport2Azure]