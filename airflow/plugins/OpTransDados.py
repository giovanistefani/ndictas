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


class OperatorTransBeneficiario(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorTransBeneficiario, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='BENEFICIARIO.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
            def rowmapper(row):
                transmf = {'MASCULINO': 'M', 'FEMININO': 'F'}
                strnull = {'NULL':''}
                return [row[0].strip(),row[1].strip(),row[2].strip(),row[3].strip(),
                        row['nome'].strip(),row[5].strip(),row[6].strip(),row[7].strip(),
                        row[8].strip(),
                        strnull[row['copart_percentual'].strip()] if row['copart_percentual'].strip() in strnull else row['copart_percentual'].strip(),
                        strnull[row['limite_copart'].strip()] if row['limite_copart'].strip() in strnull else row['limite_copart'].strip(),
                        row[11].strip(),
                        row[12].strip(),row[13].strip(),row[14].strip(),
                        strnull[row['dt_exclusao'].strip()] if row['dt_exclusao'].strip() in strnull else row['dt_exclusao'].strip(),
                        row[16],
                        transmf[row['sexo'].strip()] if row['sexo'].strip() in transmf else None,
                        row[18].strip(),row[19].strip(),row[20].strip(),row[21].strip(),
                        row[22].strip(),row[23].strip(),row[24].strip(),row[25].strip(),
                        row[26].strip(),row[27].strip(),row[28].strip(),row[29].strip()
                       ]

            table1 = etl.rowmap(table, rowmapper,
                                header=['pk_beneficiario','nr_beneficiario','nr_beneficiario_tit','fk_empresa',
                                        'nome','cpf','dt_nascimento','cod_plano',
                                        'descricao_plano','copart_percentual','limite_copart','tipo_acomodacao',
                                        'abrangencia_plano','grau_dependencia','dt_inclusao','dt_exclusao',
                                        'nome_mae', 'sexo','tipo_contrato','endereco','bairro','cidade',
                                        'uf','cep','tipo_cliente','nr_cartaonacionalsaude',
                                        'plano_regulamentado','descricao_entidade','tipo_pessoa','acao_judicial'
                                       ])
                
            table2 = etl.addfields(table1,[('fk_operadora',_SOURCE)])
            etl.tocsv(table2, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

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

class OperatorTransConsulta(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorTransConsulta, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='CONSULTAS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')

            def rowmapper(row):
                strnull = {'NULL':''}
                return [row[0].strip(),
                        row[1].strip(),
                        strnull[row['grupo_dictas'].strip()] if row['grupo_dictas'].strip() in strnull else row['grupo_dictas'].strip(),
                        ]

            table1 = etl.rowmap(table, rowmapper,
                        header=['fk_servico','tipo','grupo_dictas'
                               ])
                
            table2 = etl.addfields(table1,[('fk_operadora',_SOURCE)])
            etl.tocsv(table2, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

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

class OperatorTransCusto(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorTransCusto, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='COSTOS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')
        
        

            def rowmapper(row):
                strnull = {'NULL':''}
                return [row[0].strip(),
                        row[1].strip(),
                        row[2].strip(),
                        row[3].strip(),
                        strnull[row['nr_guia_principal'].strip()] if row['nr_guia_principal'].strip() in strnull else row['nr_guia_principal'].strip(),
                        row[5].strip(),
                        strnull[row['fk_cid'].strip()] if row['fk_cid'].strip() in strnull else row['fk_cid'].strip(),
                        strnull[row['dt_ini_internacao'].strip()] if row['dt_ini_internacao'].strip() in strnull else row['dt_ini_internacao'].strip(),
                        strnull[row['dt_fim_internacao'].strip()] if row['dt_fim_internacao'].strip() in strnull else row['dt_fim_internacao'].strip(),
                        strnull[row['fk_medicosolicitante'].strip()] if row['fk_medicosolicitante'].strip() in strnull else row['fk_medicosolicitante'].strip(),
                        strnull[row['cod_executante'].strip()] if row['cod_executante'].strip() in strnull else row['cod_executante'].strip(),
                        row[11].strip(),
                        row[12].strip(),
                        row[13].strip(),
                        row[14].strip(),
                        row[15].strip(),
                        row[16].strip(),
                        strnull[row['qtd_unica'].strip()] if row['qtd_unica'].strip() in strnull else row['qtd_unica'].strip(),
                        row[18].strip(),
                        row[19].strip(),
                        row[20].strip(),
                        row[21].strip(),
                        row[22].strip(),
                        row[23].strip(),
                        row[24].strip(),
                        strnull[row['tipo_internacao'].strip()] if row['tipo_internacao'].strip() in strnull else row['tipo_internacao'].strip(),
                        strnull[row['tipo_acomodacao'].strip()] if row['tipo_acomodacao'].strip() in strnull else row['tipo_acomodacao'].strip(),
                        strnull[row['ds_indicacao_clinica'].strip()] if row['ds_indicacao_clinica'].strip() in strnull else row['ds_indicacao_clinica'].strip(),
                        row[28].strip(),
                        row[29].strip(),
                        row[30].strip(),
                        strnull[row['partic_medico'].strip()] if row['partic_medico'].strip() in strnull else row['partic_medico'].strip()
                        ]

            table1 = etl.rowmap(table, rowmapper,
                        header=['cod_custo','fk_beneficiario','mes_competencia','nr_guia','nr_guia_principal',
                                'dt_realizacao','fk_cid','dt_ini_internacao','dt_fim_internacao','fk_medicosolicitante',
                                'cod_executante','fk_local_atend','fk_capa_unico','fk_destino_pgto','fk_servico',
                                'cod_servico_tipo','qtd','qtd_unica','vl_unitario','vl_total','vl_coparticipacao',
                                'ind_internacao','tipo_registro','tipo_atendimento','tipo_guia','tipo_internacao',
                                'tipo_acomodacao','ds_indicacao_clinica','vl_hm','vl_co','vl_filme','partic_medico'
                               ])
                
            table2 = etl.addfields(table1,[('fk_operadora',_SOURCE)])
            etl.tocsv(table2, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

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

class OperatorTransDiarias(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorTransDiarias, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='DIARIAS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')

            def rowmapper(row):
                strnull = {'NULL':''}
                return [row[0].strip(),
                        row[1].strip(),
                        row[2].strip(),
                    ]          

            table1 = etl.rowmap(table, rowmapper,
                    header=['fk_servico','tipo','grupo_dictas'
                           ])
                
            table2 = etl.addfields(table1,[('fk_operadora',_SOURCE)])
            etl.tocsv(table2, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

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


class OperatorTransEmpresas(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorTransEmpresas, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='EMPRESA.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')

            def rowmapper(row):
                strnull = {'NULL':''}
                return [row[0].strip(),
                        row[1].strip(),
                        row[2].strip(),
                        row[3].strip()
                    ]          

            table1 = etl.rowmap(table, rowmapper,
                    header=['pk_empresa','nome','razao_social','cnpj'])
                
            table2 = etl.addfields(table1,[('fk_operadora',_SOURCE)])
            etl.tocsv(table2, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

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

class OperatorTransPrestadores(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorTransPrestadores, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='PRESTADORES.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')

            def rowmapper(row):
                strnull = {'NULL':''}
                return [row[0].strip(),
                        row[1].strip(),
                        strnull[row['nome_completo'].strip()] if row['nome_completo'].strip() in strnull else row['nome_completo'].strip(),
                        row[3].strip(),
                        row[4].strip(),
                        strnull[row['dat_nascimento'].strip()] if row['dat_nascimento'].strip() in strnull else row['dat_nascimento'].strip(),
                        row[6].strip(),
                        row[7].strip(),
                        row[8].strip(),
                        strnull[row['cnae'].strip()] if row['cnae'].strip() in strnull else row['cnae'].strip(),
                        row[10].strip(),
                        strnull[row['grau'].strip()] if row['grau'].strip() in strnull else row['grau'].strip(),
                        row[12].strip(),
                        row[13].strip(),
                        row[14].strip(),
                        row[15].strip(),
                        row[16].strip(),
                        row[17].strip(),
                        row[18].strip(),
                        strnull[row['latitude'].strip()] if row['latitude'].strip() in strnull else row['latitude'].strip(),
                        strnull[row['longitude'].strip()] if row['longitude'].strip() in strnull else row['longitude'].strip(),
                        strnull[row['fone1'].strip()] if row['fone1'].strip() in strnull else row['fone1'].strip(),
                        strnull[row['fone2'].strip()] if row['fone2'].strip() in strnull else row['fone2'].strip(),
                        strnull[row['fone3'].strip()] if row['fone3'].strip() in strnull else row['fone3'].strip()
                    ]         

            table1 = etl.rowmap(table, rowmapper,
                    header=['pk_medico_prestador','nome_fantasia','nome_completo','cod_crm',
                            'f_crm','dat_nascimento','num_cpf_cnpj','tipo_prestador','grupo_prestador',
                            'cnae','especialidade','grau','endereço','numero','Complemento','bairro',
                            'cidade_prestador','uf_prestador','cep','latitude','longitude','fone1','fone2','fone3'
                           ])
                
            table2 = etl.addfields(table1,[('fk_operadora',_SOURCE)])
            etl.tocsv(table2, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

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

class OperatorTransReceitas(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorTransReceitas, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='RECEITAS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')

            def rowmapper(row):
                strnull = {'NULL':''}
                return [row[0].strip(),
                        row[1].strip(),
                        row[2].strip(),
                        row[3].strip(),
                        row[4].strip(),
                        strnull[row['nr_centro_custo'].strip()] if row['nr_centro_custo'].strip() in strnull else row['nr_centro_custo'].strip(),
                        strnull[row['descricao_centro_custo'].strip()] if row['descricao_centro_custo'].strip() in strnull else row['descricao_centro_custo'].strip(),
                        row[7].strip(),
                        row[8].strip(),
                        row[9].strip(),
                        strnull[row['tipo_cobranca_sub'].strip()] if row['tipo_cobranca_sub'].strip() in strnull else row['tipo_cobranca_sub'].strip()
                    ]        

            table1 = etl.rowmap(table, rowmapper,
                    header=['mes_competencia','fk_beneficiario','fk_empresa','dt_geracao_titulo',
                            'dt_pgto','nr_centro_custo','descricao_centro_custo','tipo_cobranca',
                            'vl_cobranca','vl_pago','tipo_cobranca_sub'
                           ])
                
            table2 = etl.addfields(table1,[('fk_operadora',_SOURCE)])
            etl.tocsv(table2, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

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

class OperatorTransServicos(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorTransServicos, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='SERVICOS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')

            def rowmapper(row):
                strnull = {'NULL':''}
                return [row[0].strip(),
                        row[1].strip(),
                        row[2].strip(),
                        row[3].strip(),
                        row[4].strip(),
                        strnull[row['subgrupo'].strip()] if row['subgrupo'].strip() in strnull else row['subgrupo'].strip(),
                        row[6].strip(),
                        strnull[row['dt_alteracao'].strip()] if row['dt_alteracao'].strip() in strnull else row['dt_alteracao'].strip(),
                        strnull[row['ind_cirurgico'].strip()] if row['ind_cirurgico'].strip() in strnull else row['ind_cirurgico'].strip()
                    ]      

            table1 = etl.rowmap(table, rowmapper,
                    header=['pk_servico','tipo','descricao','capitulo',
                            'grupo','subgrupo','dt_inclusao','dt_alteracao','ind_cirurgico'
                           ])
                
            table2 = etl.addfields(table1,[('fk_operadora',_SOURCE)])
            etl.tocsv(table2, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

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

class OperatorTransVidas(ConnAzure2AzureOperator):
    @apply_defaults
    def __init__(self,
                 previous_task,
                 *args,
                 **kwargs):
        super(OperatorTransVidas, self).__init__(*args, **kwargs)
        self.previous_task = previous_task

    def execute(self, context):
        try:
            nm_arq='VIDAS.csv'
            with open(f'{_PROC_FILES}/{nm_arq}', 'wb') as data_from:
                data_from.write(self.client_from.get_blob_client(nm_arq).download_blob().readall())
            table = etl.fromcsv(f'{_PROC_FILES}/{nm_arq}', delimiter='|')

            def rowmapper(row):
                strnull = {'NULL':''}
                transatv = {'NÃO ATIVO': 'I', 'ATIVO': 'A'}
                return [row[0].strip(),
                        row[1].strip(),
                        transatv[row['ativo'].strip()] if row['ativo'].strip() in transatv else None
                    ]      

            table1 = etl.rowmap(table, rowmapper,
                    header=['mes_competencia','fk_beneficiario','ativo'
                           ])
                
            table2 = etl.addfields(table1,[('fk_operadora',_SOURCE)])
            etl.tocsv(table2, f'{_PROC_FILES}/t{nm_arq}', delimiter='|')

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
    operators = [OperatorTransBeneficiario, OperatorTransConsulta, OperatorTransCusto, OperatorTransDiarias, OperatorTransEmpresas, OperatorTransPrestadores, OperatorTransReceitas, OperatorTransServicos, OperatorTransVidas]