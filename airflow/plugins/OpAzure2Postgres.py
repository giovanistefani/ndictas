import os
import re

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from azure.storage.blob import ContainerClient
from sqlalchemy import create_engine

import pandas as pd
from datetime import datetime
import json

from dictas import dataset, util

_AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
_TEMP_FILE = f'{_AIRFLOW_HOME}/tempfiles'


class ConnectAzure2Postgres(BaseOperator):
    @apply_defaults
    def __init__(self,
                 account_name,
                 account_key,
                 container,
                 user,
                 password,
                 link,
                 port,
                 db,
                 *args,
                 **kwargs):
        super (ConnectAzure2Postgres, self).__init__ (*args, **kwargs)
        self.client = ContainerClient (account_url = f"https://{account_name}.blob.core.windows.net/",
                                       credential = account_key, container_name = container)
        self.engine = create_engine (f'postgresql://{user}:{password}@{link}:{port}/{db}')


class CompareAzure2PostgresOperator (ConnectAzure2Postgres) :
    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs) :
        super (CompareAzure2PostgresOperator, self).__init__ (*args, **kwargs)

    def execute(self, context) :
        nmfileinfo = 'infotb/loadinfo.json'
        ldiff = self._getdiff(nmfileinfo)
        lvalidate = self._comparedb(ldiff)
        return lvalidate

    def _getdiff(self,nmfileinfo):
        ldiff = []
        dateformatinfo = '%d/%m/%Y, %H:%M:%S %z'
        from_files = sorted ([file for file in self.client.list_blobs ()], key = lambda k :k['size'])
        try :
            loadinfo = json.loads (self.client.get_blob_client (nmfileinfo).download_blob ().readall ())
            for filelake in from_files :
                for filedb in loadinfo.items () :
                    if ((filelake['name'] == filedb[0] and filelake['last_modified']>datetime.strptime (
                            f"{filedb[1]['dtinsert']} +00:00", dateformatinfo)) or (
                            filelake['name'] not in list (loadinfo.keys ())+[nmfileinfo])) :
                        ldiff.append (self._check_servico (filelake['name']))
            self.logger.info (f'Diff list {set (ldiff)}')
        except :
            ldiff = [self._check_servico (file['name']) for file in from_files if file['name'] != nmfileinfo]
            self.logger.info (f'Diff list if not exists {set (ldiff)}')
        return set(ldiff)

    def _comparedb(self,ldiff):
        lvalidate = []
        dictmax = {}
        for file in set (ldiff) :
            tbname = f"bt_{dataset.tbname (file)}"
            tprun = dataset.consult (file)['typerun']
            if tprun == 'increment' :
                try:
                    maxval = dictmax[tbname]
                except :
                    conn = self.engine.connect ()
                    selectmax = f'select max(mes_competencia) as  mes_competencia from dictas.{tbname}'
                    maxval = conn.execute (selectmax).fetchall ()[0][0]
                    dictmax.update({tbname:maxval})
                filedate = datetime.strptime (re.search (r"(\d+)", file.split ('/')[1].replace ('-', '')).group (),
                                              '%m%Y').date ()
                if filedate>maxval :
                    lvalidate.append (file)
            else :
                lvalidate.append (file)
        self.logger.info (f'Diff validate {lvalidate}')
        return lvalidate

    @staticmethod
    def _check_servico(file) :
        if (file.lower ().find ('servico')>=0 or
                file.lower ().find ('consulta')>=0 or
                file.lower ().find ('diaria')>=0) :
            return 'servico/SERVICO_CONSOLIDADO.pkl'
        else:
            return file


class LoadAzure2PostgresOperator (ConnectAzure2Postgres) :
    @apply_defaults
    def __init__(self,
                 previous_task,
                 extension,
                 chunksize,
                 *args,
                 **kwargs) :
        super (LoadAzure2PostgresOperator, self).__init__ (*args,**kwargs)
        self.previous_task = previous_task
        self.extension = extension
        self.temp_dir = _TEMP_FILE
        self.chunksize = chunksize

    def execute(self, context) :
        listrun = context['ti'].xcom_pull (task_ids = self.previous_task)
        dictload={}
        for nmfile in listrun :
            infoload = self._execute_file (nmfile)
            loadinfo = {nmfile :infoload}
            dictload.update(loadinfo)
            self.logger.info (f'dictload {dictload}')
        try:
            loadinfofile = json.loads (self.client.get_blob_client ('infotb/loadinfo.json').download_blob ().readall ())
            fullfile = loadinfofile.update (dictload)
            self.logger.info (f'full file sucess {fullfile}')
        except:
            fullfile = dictload
            self.logger.info (f'full file fail {fullfile}')
        jsondictload = json.dumps (fullfile)
        self.logger.info (f'json dictasload {jsondictload}')
        self.client.get_blob_client ('infotb/loadinfo.json').upload_blob (jsondictload, overwrite = True)

    def _execute_file(self,nmfile):
        file = nmfile.split ('/')[1].replace (f'.{self.extension}', '')
        filelocal = f'{self.temp_dir}/{file}.{self.extension}'
        tbname = f'bt_{dataset.tbname (nmfile)}'
        self.logger.info (f'file {file}\n filelocal {filelocal}\n tbname {tbname}')
        df = self._readfile (filelocal, nmfile)
        self.logger.info (f'shape {df.shape}')
        if_exists = self.check_exists (nmfile)
        conn = self.engine.connect ()
        sqlcount = conn.execute (f'select count(*) from dictas.{tbname}').fetchall ()
        self.logger.info (sqlcount)
        if df.shape[0]>0:
            infoload = self._loaddb (df, tbname, if_exists, nmfile)
            self.logger.info (f'load complete')
        else:
            infoload = dict (dtinsert = datetime.today ().strftime ("%d/%m/%Y, %H:%M:%S"), status = 'empty')
            self.logger.info (f'table empty')
        os.remove (filelocal)
        self.logger.info (sqlcount)
        return infoload

    def _readfile(self,filelocal,nmfile):
        with open (filelocal, 'wb') as data_from :
            data_from.write (self.client.get_blob_client (nmfile).download_blob ().readall ())
        return pd.read_pickle (filelocal)

    def _loaddb(self,df,tbname,if_exists,nmfile):
        fk_operadora = int (df.head (1)['fk_operadora'][0])
        print (f'fk_operadora {fk_operadora}')
        if if_exists == 'replace' :
            conn = self.engine.connect ()
            conn.execute (f"delete from dictas.{tbname} where fk_operadora = {fk_operadora}")

        self._loadparts (df, tbname)
        infoload = dict (dtinsert = datetime.today ().strftime ("%d/%m/%Y, %H:%M:%S"), status = 'sucess')
        '''
        try:
            self._loadparts(df,tbname)
            infoload = dict (dtinsert = datetime.today ().strftime ("%d/%m/%Y, %H:%M:%S"),
                             status = 'sucess')
            self.logger.info (f'load part sucess')
        except:
            infoload = dict (dtinsert = datetime.today ().strftime ("%d/%m/%Y, %H:%M:%S"),
                             status = 'fail')
            self.logger.info (f'load part fail')
        '''
        return infoload

    def _upload_azure(self,nm_file):
        file = nm_file.split ('/')[1].replace ('pkl', '')
        file_upload = nm_file
        with open (f'{self.temp_dir}/{file}.{self.extension_to}', "rb") as data_to :
            self.client_to.get_blob_client (file_upload).upload_blob (data_to, overwrite = True)
        os.remove (f'{self.temp_dir}/{file}.{self.extension_to}')

    def _loadparts(self,df,tbname):
        for start_range in range (0, df.shape[0], self.chunksize) :
            if start_range != 0 :
                start_range += 1
            end_range = start_range+self.chunksize
            dfload = df.iloc[start_range :end_range]
            dfload.to_sql (tbname, con = self.engine,
                           if_exists = 'append',
                           chunksize = self.chunksize, index = False,
                           schema = 'dictas', method = 'multi')

    @staticmethod
    def check_exists(nmfile):
        if dataset.consult (nmfile)['typerun'] == 'full' :
            return 'replace'
        else :
            return 'append'


class Azure2PostgresPlugin (AirflowPlugin) :
    """
    Generate plugin operator.
    """
    name = "azure2postgres"
    operators = [LoadAzure2PostgresOperator, CompareAzure2PostgresOperator]