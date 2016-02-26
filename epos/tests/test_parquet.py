import pandas as pd
from epos.odo.parquet import Parquet, resource
from odo import HDFS, odo
from odo.backends.tests.conftest import sqlctx

data = [['Alice', 100.0, 1],
        ['Bob', 200.0, 2],
        ['Alice', 50.0, 3]]


df = pd.DataFrame(data, columns=['name', 'amount', 'id'])


def test_resource():
    assert isinstance(resource('foo.parquet'), Parquet)
    assert isinstance(resource('/path/to/foo.parquet'), Parquet)


def test_hdfs_resource():
    auth = {'user': 'hdfs', 'port': 14000, 'host': 'hostname'}
    tmp = resource('hdfs://foo.parquet', **auth)
    assert isinstance(tmp, HDFS(Parquet))
    assert tmp.path == 'foo.parquet'


def test_convert_df_to_parquet(tmpdir):
    path = str(tmpdir.join("output.parquet"))
    print path
    sdf = odo(df, sqlctx)
    odo(sdf, path)
