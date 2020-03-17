"""
Import files from Azure Blob Storage to Azure Blob Storage.

This operator is useful for transporting and transferring
data across datalake environments.
"""


import os
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from azure.storage.blob import ContainerClient

import pandas as pd
import csv

from dictas import processor, dataset, util

_AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
_TEMP_FILE = f'{_AIRFLOW_HOME}/tempfiles'


class ConnectAzure2AzureOperator (BaseOperator) :
    """
    Compare the previous environment and generate
    list with the files that should be executed.
    """
    @apply_defaults
    def __init__(self,
                 account_name,
                 account_key,
                 container_from,
                 container_to,
                 extension_from,
                 extension_to,
                 *args,
                 **kwargs) :
        super (ConnectAzure2AzureOperator, self).__init__ (*args, **kwargs)
        self.client_from = ContainerClient (account_url = f"https://{account_name}.blob.core.windows.net/",
                                            credential = account_key, container_name = container_from)
        self.client_to = ContainerClient (account_url = f"https://{account_name}.blob.core.windows.net/",
                                          credential = account_key, container_name = container_to)
        self.extension_from = extension_from
        self.extension_to = extension_to
        self.container_from = container_from
        self.container_to = container_to


class CompareAzure2AzureOperator (ConnectAzure2AzureOperator) :
    """
    Compare the previous environment and generate
    list with the files that should be executed.
    """
    @apply_defaults
    def __init__(self,
                 num_workers,
                 tb_type=None,
                 *args,
                 **kwargs) :
        super (CompareAzure2AzureOperator, self).__init__ (*args, **kwargs)
        self.tb_type = tb_type
        self.num_workers = num_workers

    def execute(self, context) :

        ldiff = []

        from_files = sorted ([file for file in self.client_from.list_blobs ()], key = lambda k :k['size'])
        to_files = sorted ([file for file in self.client_to.list_blobs ()], key = lambda k :k['size'])

        list_name_to = [self._replaceextension (file['name']) for file in to_files]

        for file_from in from_files :
            if self._replaceextension (file_from['name']) not in list_name_to :
                ldiff.append (file_from['name'])
            for file_to in to_files :
                if self._replaceextension (file_from['name']) == self._replaceextension (file_to['name']) :
                    if file_from['last_modified']>file_to['last_modified'] :
                        ldiff.append (file_from['name'])
        self.logger.info (f'Genereate Diff List {ldiff}')

        if self.container_to == 'refined' :
            ldiff = [file for file in ldiff if file.find ('error')<0]

        return util.chunklist (ldiff, self.num_workers)

    def _replaceextension(self, file) :
        return file.replace (f'.{self.extension_from}', '').replace (f'.{self.extension_to}', '')


class LoadAzure2AzureOperator (ConnectAzure2AzureOperator) :
    """
    Transporting and transforming data
    between enviroments.
    """
    @apply_defaults
    def __init__(self,
                 sep,
                 previous_task,
                 worker,
                 processor,
                 *args,
                 **kwargs) :
        super (LoadAzure2AzureOperator, self).__init__ (*args, **kwargs)
        self.sep = sep
        self.previous_task = previous_task
        self.worker = worker
        self.processor = processor
        self.temp_dir = _TEMP_FILE

    def execute(self, context) :
        # Verify how list use according worker
        listrun = util.listrun (context['ti'].xcom_pull (task_ids = self.previous_task), self.worker)

        if len (listrun)>0 :
            for nm_file in listrun :
                # Download local file
                self._download_local (nm_file)
                # Read file according type
                df = self._read_file (nm_file)
                # Transforming file according enviroment
                df = self._run_processor (nm_file, df)
                # Write file according type
                self._write_file (nm_file, df[0])
                # Upload file to Azure
                self._upload_azure (nm_file)

                if len (df)>1 :
                    nm_file_replace = nm_file.replace (f'.{self.extension_from}', '')
                    nm_file_error = f'{nm_file_replace}_error.{self.extension_from}'
                    self._write_file (nm_file_error, df[1])
                    self._upload_azure (nm_file_error)

                self.logger.info ('Publish file')
                self.logger.info ('Finish')
        else :
            self.logger.info ('Dont files to run')

    def _download_local(self,nm_file):
        print(f'file {nm_file}')
        file = nm_file.split ('/')[1].replace (f'.{self.extension_from}', '').replace (f'.{self.extension_to} ', '')
        self.logger.info (f'run {nm_file}')
        with open (f'{self.temp_dir}/{file}.{self.extension_from}', 'wb') as data_from :
            data_from.write (self.client_from.get_blob_client (nm_file).download_blob ().readall ())
        self.logger.info (f'write local file {nm_file}')

    def _read_file(self,nm_file):
        file = nm_file.split ('/')[1].replace (f'.{self.extension_from}', '').replace (f'.{self.extension_to} ', '')
        if self.extension_from == 'csv' :
            enc = util.find_encoding(f'{self.temp_dir}/{file}.{self.extension_from}')

            df = pd.read_csv (f'{self.temp_dir}/{file}.{self.extension_from}',
                              sep = self.sep,
                              header = None,
                              error_bad_lines = False,
                              encoding = enc,
                              quoting = csv.QUOTE_NONE,
                              low_memory = False,
                              na_values = ['NULL\t','NULL','\t','','nan'])
            os.remove (f'{self.temp_dir}/{file}.{self.extension_from}')
            self.logger.info (f'Create df {nm_file} shape {df.shape}')
        elif self.extension_from == 'pkl' :
            df = pd.read_pickle (f'{self.temp_dir}/{file}.{self.extension_from}')
            os.remove (f'{self.temp_dir}/{file}.{self.extension_from}')
            self.logger.info (f'Create df {nm_file} shape {df.shape}')
        else :
            self.logger.info ('unknow type')
        return df

    def _run_processor(self,nm_file, df):
        file = nm_file.split ('/')[1].replace (f'.{self.extension_from}', '').replace (f'.{self.extension_to} ', '')
        if self.processor == 'transient2raw' :
            df = processor.Processor (df, file).transient2raw ()
            self.logger.info (f'Transform {self.processor} df {nm_file} ')
        elif self.processor == 'raw2trusted' :
            df = processor.Processor (df, file).raw2trusted ()
            self.logger.info (f'Transform {self.processor} df {nm_file} ')
        elif self.processor == 'trusted2refined' :
            df = processor.Processor (df, file).trusted2refined ()
            self.logger.info (f'Transform {self.processor} df {nm_file} shape {len(df)}')
        else :
            self.logger.info ('unknow processor')
        return df

    def _write_file(self,nm_file,df):
        file = nm_file.split ('/')[1].replace (f'.{self.extension_from}', '').replace (f'.{self.extension_to} ', '')
        if self.extension_to == 'csv' :
            df.to_csv (f'{self.temp_dir}/{file}.{self.extension_to}')
            self.logger.info ('Create temporary file')
        else :
            df.to_pickle (f'{self.temp_dir}/{file}.{self.extension_to}')
            self.logger.info ('Create temporary file')

    def _upload_azure(self,nm_file):
        file = nm_file.split ('/')[1].replace (f'.{self.extension_from}', '').replace (f'.{self.extension_to} ', '')
        file_upload = nm_file.replace (self.extension_from, self.extension_to)
        with open (f'{self.temp_dir}/{file}.{self.extension_to}', "rb") as data_to :
            self.client_to.get_blob_client (file_upload).upload_blob (data_to, overwrite = True)
        os.remove (f'{self.temp_dir}/{file}.{self.extension_to}')


class DeleteFilesAzure2AzureOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 account_name,
                 account_key,
                 container,
                 *args,
                 **kwargs) :
        super (DeleteFilesAzure2AzureOperator, self).__init__ (*args, **kwargs)
        self.client = ContainerClient (account_url = f"https://{account_name}.blob.core.windows.net/",
                                       credential = account_key,
                                       container_name = container)

    def execute(self,context):
        for file in self.client.list_blobs () :
            bob_cilent = self.client.get_blob_client (file)
            bob_cilent.delete_blob ()


class Azure2AzurePlugin (AirflowPlugin) :
    """
    Generate plugin operator.
    """
    name = "azure2azure"
    operators = [CompareAzure2AzureOperator, LoadAzure2AzureOperator, DeleteFilesAzure2AzureOperator]
