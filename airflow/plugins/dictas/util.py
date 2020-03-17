import sys
import re
import logging
import chardet
from datetime import datetime

logging.basicConfig (stream = sys.stdout, level = logging.INFO, format = '%(asctime)s;%(levelname)s;%(message)s',
                     datefmt = '%m/%d/%Y %I:%M:%S %p')


def replacevalue(df, val, replace_val) :
    for col in df.columns :
        df[col] = df[col].apply (lambda x :re.sub (val, replace_val, str (x)))
    return df


def is_int(val) :
    try :
        int (val)
        return True
    except ValueError :
        return False


def is_float(val) :
    try :
        float (val)
        return True
    except ValueError :
        return False


def is_datetime(val, format) :
    try :
        datetime.strptime (val, format)
        return True
    except ValueError :
        return False


def is_null(val) :

    if val and val != 'nan' :
        return True
    else :
        return False


def listrun(listexecute, workers) :
    poslist = int (workers.replace ('worker_',''))-1
    return listexecute[poslist]


def chunklist(lst,chunk):
    return [lst[num::chunk] for num in range(chunk)]


def find_encoding(fname):
    r_file = open(fname, 'rb').read()
    result = chardet.detect(r_file)
    charenc = result['encoding']
    return charenc


'''
Contar linhas do csv
'''

def line_count(fname):
    return sum(1 for line in open(fname))

    