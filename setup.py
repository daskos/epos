#!/usr/bin/env python

from os.path import exists

from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['-v']
        self.test_suite = True

    def run_tests(self):
        import pytest
        import sys

        errno = pytest.main(self.test_args)
        self.handle_exit()
        sys.exit(errno)

    @staticmethod
    def handle_exit():
        import atexit
        atexit._run_exitfuncs()


setup(name='epos',
      version='0.1',
      description='DAG Task scheduler and DSL on top of Mesos',
      url='http://github.com/lensacom/epos',
      maintainer='Krisztian Szucs',
      maintainer_email='szucs.krisztian@gmail.com',
      license='BSD',
      keywords='task-scheduling parallelism mesos spark',
      packages=['epos', 'epos.odo'],
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      install_requires=['toolz', 'dask', 'odo',
                        'dask.mesos'],
      cmdclass={'test': PyTest},
      tests_require=['pytest', 'pywebhdfs', 'pymongo',
                     'kazoo', 'sqlalchemy', 'paramiko',
                     'cassandra-driver'],
      dependency_links=[
          'git+https://github.com/lensacom/dask.mesos.git#egg=dask.mesos-0.1'],
      zip_safe=False)
