#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import charon

#extras_require = {
#  'spark': ['pyspark', 'toolz >= 0.7.2'],
#  'bag': ['cloudpickle', 'toolz >= 0.7.2', 'partd >= 0.3.2'],
#  'dataframe': ['numpy', 'pandas >= 0.16.0', 'toolz >= 0.7.2', 'partd >= 0.3.2'],
#}
#extras_require['complete'] = sorted(set(sum(extras_require.values(), [])))

setup(name='charon',
      version='0.1',
      description='DAG Task scheduler and DSL on top of Mesos',
      url='http://github.com/lensacom/charon',
      maintainer='Krisztian Szucs',
      maintainer_email='szucs.krisztian@gmail.com',
      license='BSD',
      keywords='task-scheduling parallelism mesos spark',
      packages=['charon'],
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      install_requires=['toolz', 'dask', 'odo'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'],
      zip_safe=False)
