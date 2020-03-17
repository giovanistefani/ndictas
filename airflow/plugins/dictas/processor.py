import sys
import logging
import re
import pandas as pd
import numpy as np

from dictas import dataset, util

logging.basicConfig (
    stream = sys.stdout,
    level = logging.INFO,
    format = '%(asctime)s;%(levelname)s;%(message)s',
    datefmt = '%m/%d/%Y %I:%M:%S %p'
)

logger = logging.getLogger ('processor')


def consolidateservico(ldf):
    df = pd.concat(ldf[0:1],ignore_index=True)
    df = ldf[2].merge(df, how='left', on=['pk_servico', 'tipo'])
    df = df.groupby(['pk_servico','fk_operadora','tipo']).tail(1)
    return df


class Processor :
    def __init__(self, df, table_nm) :
        self.df = df
        self.table_nm = table_nm
        self.logger = logger
        self.df_out = []

    def transient2raw(self) :
        tb_cols = [col['nome_banco'] for col in dataset.consult (self.table_nm)['fields'].values ()]
        self.logger.info (f'List of columns dictionary {tb_cols}')
        lcol = [x for x in self.df.columns if x<len (tb_cols)]
        dict_cols = dict (zip (lcol, tb_cols))
        self.logger.info(f'Create Dictionary Cols {self.table_nm}')

        self.df_out.append(self.df.rename (columns = dict_cols))
        self.logger.info (f'Rename Cols {self.table_nm}')
        return self.df_out

    def raw2trusted(self) :
        self.df = self.df.drop_duplicates ()
        self.logger.info (f'Remove Duplicates {self.table_nm}')

        self.df = util.replacevalue (self.df, r'NULL|[\t]', '')
        self.logger.info (f'Remove Null Values and Tablespace {self.table_nm}')

        self.df = self.df.apply (
            lambda x :x.str.strip ().replace ('', None))
        self.df = self.df.where(pd.notnull(self.df),None)
        self.logger.info (f'Remove Blank Space {self.table_nm}')

        self._runvalidator(50000)
        self.logger.info (f'Create Validate Column {self.table_nm}')
        
        df_error = self.df[self.df['validate'] == False]
        self.logger.info (f'shape df error {df_error.shape} da tabela {self.table_nm}')
        
        df = self.df[self.df['validate'] == True].drop (columns = ['validate','inf_val'])
        self.logger.info (f'shape df {df.shape} da tabela {self.table_nm}')

        self.df = adjust_type (df, dataset.tbname(self.table_nm))
        self.logger.info (f'Adjust Type  {self.table_nm}')

        self.df = self.df[sel_no_int_cols (self.df.columns)]
        self.logger.info (f'Select Cols {self.table_nm}')
        self.df_out.append (self.df)

        if not df_error.empty :
            df_error = df_error[sel_no_int_cols (df_error.columns)]
            self.df_out.append (df_error)
        return self.df_out

    def trusted2refined(self) :
        if self.table_nm.lower ().find ('beneficiario')>=0 :
            self._beneficiario ()
        if self.table_nm.lower ().find ('vida')>=0 :
            self._vidas ()
        if self.table_nm.lower ().find ('custo')>=0 :
            self._custo ()
        if self.table_nm.lower ().find ('servico')>=0 :
            self._servico ()
        if self.table_nm.lower ().find ('empresa')>=0 :
            self._empresa ()
        if self.table_nm.lower ().find ('prestador')>=0 :
            self._prestador ()
        if self.table_nm.lower ().find ('receita')>=0 :
            self._receita ()
        self.df_out.append (self.df)
        return self.df_out

    def _beneficiario(self) :
        self.df['sexo'] = self.df['sexo'].apply (transform_sexo)
        self.df['fk_operadora'] = 17

    def _vidas(self):
        self.df['ativo'] = self.df['ativo'].apply (transform_ativo)
        self.df['mes_competencia'] = self.df['mes_competencia'].apply (mes_competencia_todate)
        self.df['fk_operadora'] = 17

    def _custo(self):
        self.df['mes_competencia'] = self.df['mes_competencia'].apply (mes_competencia_todate)
        self.df['ind_internacao'] = self.df['ind_internacao'].apply(transform_ind_internacao)
        self.df['fk_operadora'] = 17

    def _servico(self):
        self.df['grupo'] = self.df['grupo'].fillna('NI')
        self.df['fk_operadora'] = 17

    def _empresa(self):
        self.df['fk_operadora'] = 17

    def _prestador(self):
        self.df['fk_operadora'] = 17

    def _receita(self):
        self.df['mes_competencia'] = self.df['mes_competencia'].apply (mes_competencia_todate)
        self.df['fk_operadora'] = 17

    def _runvalidator(self, chunksize) :
        end = 0
        chunklist = []
        for start in range (0, self.df[-1 :].index[0], chunksize) :
            if start != 0 :
                start += 1
            end += chunksize
            dfpre = self.df.loc[start :end]
            dfpre[['validate', 'inf_val']] = dfpre.apply (lambda x :validator (x, dataset.tbname (self.table_nm)),
                                                          axis = 1, result_type = 'expand')
            chunklist.append (dfpre)
            self.logger.info (f'shape df {dfpre.shape} start {start} and end {end}')
        self.df = pd.concat (chunklist, axis = 0)


def mes_competencia_todate(val) :
    if len (str (val)) == 5 :
        return pd.to_datetime (f"010{replace_nan (val, '19000101')}", format = '%d%m%Y', errors = 'coerce')
    else :
        return pd.to_datetime (f"01{replace_nan (val, '19000101')}", format = '%d%m%Y', errors = 'coerce')


def typeload(name) :
    if (name.lower ().find ('custo')>=0 or
            name.lower ().find ('receitas')>=0 or
            name.lower ().find ('vidas')>=0) :
        return 'increment'
    else :
        return 'full'


def transform_sexo(val) :
    if val.lower () == 'masculino' :
        return 'M'
    elif val.lower () == 'feminino' :
        return 'F'
    else :
        return 'FP'


def transform_ativo(tipo_pessoa):

    if tipo_pessoa == 'NÃO ATIVO':
        return 0
    elif tipo_pessoa == 'ATIVO':
        return 1
    else:
        return 2


def transform_ind_internacao(ind_internacao):
    if ind_internacao == 'NÃO':
        return 'N'
    elif ind_internacao == 'SIM':
        return 'S'
    else:
        return 2


def sel_no_int_cols(cols) :
    validatecols = []
    for col in cols :
        try :
            int (col)
        except ValueError:
            validatecols.append (col)
    return validatecols


def valnull(nullable, replace_null, val):
    if nullable == 'notnull' and replace_null=='' and not util.is_null(val):
        return False
    else:
        return True


def valtype(tipo,date_format,replace_null,val):
    if replace_null!='':
        valtipo = val.replace('nan',replace_null)
    else:
        valtipo = val
    if ((tipo=='int' and util.is_int(valtipo)) or
        (tipo=='float' and util.is_float(valtipo)) or
        (tipo=='date' and util.is_datetime(valtipo,date_format)) or
            tipo=='string'):
        return True
    else:
        return False


def valregex(regex,nullable,replace_null,val):
    if ((nullable=='null' and valnull(nullable, replace_null, val)) or
        (regex and bool(re.match(regex,val))) or
            not regex):
        return True
    else:
        return False


def validator(df, tb_name) :
    l_validator = []
    l_val = []
    for meta in dataset.consult (tb_name)['fields'].values () :
        nome_banco = meta['nome_banco']
        nullable = meta['nullable']
        replace_null = meta['replace_null']
        tipo = meta['tipo']
        date_format = meta['date_format']
        regex = meta['regex']

        try :
            int (nome_banco)
        except ValueError :
            try:
                if (valnull (nullable, replace_null, df[nome_banco]) and
                        valtype (tipo, date_format, replace_null,df[nome_banco]) and
                        valregex (regex,nullable,replace_null,df[nome_banco])) :
                    l_validator.append (True)
                    l_val.append ({nome_banco :True})
                else :
                    l_validator.append (False)
                    l_val.append ({nome_banco :False})
            except:
                l_validator.append (False)
                l_val.append ({nome_banco :False})
    if False in l_validator :
        return False, l_val
    else :
        return True, l_val


def replace_nan(val, replace_null) :
    if replace_null != '' and (val == 'nan' or not val) :
        return val.replace ('nan', replace_null)
    else :
        return val


def adjust_type(df, tb_name) :
    logger = logging.getLogger ('adjust type')
    for meta in dataset.consult (tb_name)['fields'].values () :
        nome_banco = meta['nome_banco']
        tipo = meta['tipo']
        date_format = meta['date_format']
        replace_null = meta['replace_null']
        if tipo == 'date' :
            try :
                df[nome_banco] = df[nome_banco].apply (lambda x : pd.to_datetime(replace_nan(x, replace_null) ,
                                                       format = date_format,
                                                       errors = 'coerce'))
                logger.info (f'type date sucess in {nome_banco}')
            except ValueError as ve :
                logger.info (f'{ve} type date dont recognize in {nome_banco}')
                raise
        elif tipo == 'int' :
            try :
                df[nome_banco] = df[nome_banco].apply(lambda x : replace_nan(x, replace_null)).astype (int)
                logger.info (f'type int sucess in {nome_banco}')
            except ValueError as ve :
                logger.info (f'{ve} type int dont recognize in {nome_banco}')
                raise
        elif tipo == 'float' :
            try :
                df[nome_banco] = df[nome_banco].apply(lambda x : replace_nan(x, replace_null)).astype (float)
                logger.info (f'type float sucess in {nome_banco}')
            except ValueError as ve :
                logger.info (f'{ve} type float dont recognize in {nome_banco}')
                raise
    return df
