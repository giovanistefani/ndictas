import os

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from azure.storage.blob import ContainerClient

from dictas import dataset, util

_AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
_TEMP_FILE = f'{_AIRFLOW_HOME}/tempfiles'


class ConnectLocal2AzureOperator (BaseOperator) :
    @apply_defaults
    def __init__(self,
                 account_name,
                 account_key,
                 container_name,
                 source,
                 *args,
                 **kwargs) :
        super (ConnectLocal2AzureOperator, self).__init__ (*args, **kwargs)
        self.client = ContainerClient (account_url = f"https://{account_name}.blob.core.windows.net/",
                                       credential = account_key, container_name = container_name)
        self.source = source


class CompareLocal2AzureOperator (ConnectLocal2AzureOperator) :

    @apply_defaults
    def __init__(self,
                 num_workers,
                 *args,
                 **kwargs) :
        super (CompareLocal2AzureOperator, self).__init__ (*args, **kwargs)
        self.num_workers = num_workers

    def execute(self, context) :
        ldatalake = [file['name'] for file in self.client.list_blobs ()]
        self.logger.info ('Create Datalake List')

        llocal = [os.path.join (currentpath, file).replace (self.source, '')[1 :] for currentpath, folders, files in
            os.walk (self.source) for file in files if file != 'diff.txt' and file.find ('meta')<0]
        self.logger.info (f'Create Local List {llocal}')

        ldiffinc = [file for file in llocal if dataset.consult(file)['typerun']  == 'increment' and
                                               file not in ldatalake]
        ldifffull = [file for file in llocal if dataset.consult(file)['typerun'] == 'full']
        ldiff = ldiffinc+ldifffull
        self.logger.info (f'Genereate Diff List {ldiff}')
        return util.chunklist(ldiff,self.num_workers)


class LoadLocal2AzureOperator (ConnectLocal2AzureOperator) :

    @apply_defaults
    def __init__(self,
                 previous_task,
                 worker,
                 *args,
                 **kwargs) :
        super (LoadLocal2AzureOperator, self).__init__ (*args, **kwargs)
        self.previous_task = previous_task
        self.worker = worker

    def execute(self, context) :
        listrun = util.listrun (context['ti'].xcom_pull (task_ids = self.previous_task), self.worker)
        if len (listrun)>0 :
            for nm_file in listrun :
                self.logger.info (f'run {nm_file}')
                blob = self.client.get_blob_client (nm_file)
                upload_file = f'{self.source}/{nm_file}'
                with open (upload_file, "rb") as data :
                    blob.upload_blob (data, overwrite = True)
                self.logger.info (f'Upload file {nm_file}')
        else :
            self.logger.info ('Not files in run')


class Local2AzurePlugin(AirflowPlugin):
    name = "local2azure"
    operators=[CompareLocal2AzureOperator,LoadLocal2AzureOperator]