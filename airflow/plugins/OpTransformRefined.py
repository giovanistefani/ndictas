import os

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from azure.storage.blob import ContainerClient

import pandas as pd

from dictas import processor

_AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
_TEMP_FILE = f'{_AIRFLOW_HOME}/tempfiles'


class ConsolidateServicoOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 account_name,
                 account_key,
                 container,
                 listfiles,
                 dir_out,
                 nm_out,
                 extension,
                 previous_task,
                 *args,
                 **kwargs) :
        super (ConsolidateServicoOperator, self).__init__ (*args, **kwargs)
        self.client = ContainerClient (account_url = f"https://{account_name}.blob.core.windows.net/",
                                       credential = account_key,
                                       container_name = container)
        self.listfiles = listfiles
        self.extension = extension
        self.dir_out = dir_out
        self.nm_out = nm_out
        self.previous_task = previous_task
        self.temp_dir = _TEMP_FILE

    def execute(self,context):
        listrun = context['ti'].xcom_pull (task_ids = self.previous_task)
        listrun = listrun[0]+listrun[1]+listrun[2]
        intersect = [file for file in listrun if file in self.listfiles]
        self.logger.info (f'list intersect {intersect}')
        if len(intersect)>0:
            self._download_local()
            ldf = self._read_file()
            localfile = f'{self.temp_dir}/{self.nm_out}.{self.extension}'
            outfile = f'{self.dir_out}/{self.nm_out}.{self.extension}'
            processor.consolidateservico(ldf).to_pickle(localfile)
            with open (localfile, "rb") as data_to :
                self.client.get_blob_client (outfile).upload_blob (data_to, overwrite = True)
            os.remove (localfile)

    def _download_local(self):
        for nm_file in self.listfiles :
            file = nm_file.split ('/')[1]
            self.logger.info (f'run {nm_file}')
            with open (f'{self.temp_dir}/{file}', 'wb') as data_from :
                data_from.write (self.client.get_blob_client (nm_file).download_blob ().readall ())
            self.logger.info (f'write local file {nm_file}')

    def _read_file(self):
        ldf = []
        for nm_file in self.listfiles :
            file = f"{self.temp_dir}/{nm_file.split ('/')[1]}"
            df = pd.read_pickle(file)
            os.remove (file)
            ldf.append(df)
        return ldf


class TransformRefinedPlugin (AirflowPlugin) :
    """
    Generate plugin operator.
    """
    name = "transformrefined"
    operators = [ConsolidateServicoOperator]