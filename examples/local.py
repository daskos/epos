from debas.imperative import mesos
from debas.spark import spark
from odo import odo

import pandas as pd

@mesos(pure=True)
def download(url):
    return odo(url, 'sales.csv')

@mesos(pure=True)
def summarize(csv):
    df = odo(csv, pd.DataFrame)
    summary = df.groupby(['Product', 'Payment_Type']).sum()
    return odo(summary, 'summary.jsonlines')

@mesos(pure=True)
@spark()
def spark_maximum(sc, sqlctx, json):
    rdd = odo(json, sc)
    return rdd.map(lambda x: x['Price']).max() + 200

@mesos(pure=True)
def maximum(json):
    lst = odo(json, [])
    prices = map(lambda x: x['Price'], lst)
    return max(prices)

@mesos(pure=True)
def aggregate(first, second):
    return first+second


if __name__ == '__main__':
    url = "http://samplecsvs.s3.amazonaws.com/SalesJan2009.csv"
    csv = download(url)
    summary = summarize(csv)
    max1 = maximum(summary)
    max2 = spark_maximum(summary)
    summ = aggregate(max1, max2 + 30)

    print summ.compute()
