#!/usr/bin/env python

from os.path import exists

from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['-vs', 'epos/']
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


extras_require = {
    'backends': ['odo', 'pywebhdfs', 'pymongo', 'sqlalchemy', 'paramiko',
                 'cassandra-driver', 'pykafka'],
    'mesos': ['dask.mesos>=0.2.1', 'satyr>=0.2', 'requests']
}
extras_require['complete'] = sorted(set(sum(extras_require.values(), [])))


setup(name='epos',
      version='0.1',
      description='DAG Task scheduler and DSL on top of Mesos',
      url='http://github.com/lensacom/epos',
      maintainer='Krisztian Szucs',
      maintainer_email='szucs.krisztian@gmail.com',
      license='BSD',
      keywords='task-scheduling parallelism mesos spark',
      packages=['epos', 'epos.odo', 'epos.cli'],
      long_description=(open('README.md').read() if exists('README.md')
                        else ''),
      cmdclass={'test': PyTest},
      install_requires=extras_require['mesos'] + ['click'],
      extras_require=extras_require,
      tests_require=['pytest-mock', 'pytest'],
      entry_points='''
          [console_scripts]
          epos=epos.cli:epos
          chronos=epos.cli:chronos
          marathon=epos.cli:marathon
      ''',
      zip_safe=False)
