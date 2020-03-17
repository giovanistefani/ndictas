#! /usr/bin/env python3
import os

from setuptools import setup

BASE_DIR = os.path.dirname(__file__)
__VERSION__ = '0.0.1'

setup(name='jupyter_etl_dictas',
      version=__VERSION__,
      author='Softplan',
      author_email='dictas@softplan.com.br',
      license=open(os.path.join(BASE_DIR, 'LICENSE')).read(),

      #packages=['jupyter_etl_dictas'],
      #namespace_packages=['jupyter_etl_dictas'],
      zip_safe=False)
