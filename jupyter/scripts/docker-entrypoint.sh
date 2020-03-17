#!/bin/bash

. .env

python setup.py -q develop

if [ "$1" = 'jupyter' ]; then
  jupyter notebook --ip=0.0.0.0 --port=$JUPYTER_PORT --allow-root \
                                --NotebookApp.notebook_dir='./notebook' \
                                --NotebookApp.token='' \
                                --NotebookApp.password=''
fi

if [ "$1" = 'tests' ]; then
  ENVIRONMENT=test py.test --cov=vda.sac ${2:-tests/unit/}
  rm -f .coverage .coverage.*  # cleanup
fi

if [ "$1" = 'bash' ]; then
  /bin/bash
fi
