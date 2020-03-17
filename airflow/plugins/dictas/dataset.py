import os
import json

_AIRFLOW_HOME = os.getenv ('AIRFLOW_HOME')
_LOCATION = f'{_AIRFLOW_HOME}/metadado'

with open (os.path.join (_LOCATION, "metadados.json"), "rb") as meta :
    metadados = json.load (meta)


def consult(name) :
    for table in metadados :
        if name.lower ().find (table)>=0 :
            return metadados[table]


def tbname(name) :
    for table in metadados:
        if name.lower().find(table)>=0:
            return table
