FROM lensa/pyspark:pip-2-3-spark-2.0.0-SNAPSHOT-hdfs-client-cdh5.7-java-8-mesos-0.28.1-2.0.20-ubuntu-15.04

RUN apt-get update && apt-get install -y libssl-dev librdkafka-dev
RUN pip install cloudpickle && \
  wget http://downloads.mesosphere.io/master/debian/8/mesos-0.27.0-py2.7-linux-x86_64.egg -O mesos.egg && \
  easy_install mesos.egg && \
  rm /mesos.egg

RUN pip install pywebhdfs pymongo sqlalchemy paramiko cassandra-driver pykafka requests odo toolz
ADD . /epos

RUN pip install git+https://github.com/lensacom/dask.mesos git+https://github.com/lensacom/satyr
RUN pip install -e /epos[complete]

ENV PYTHONPATH /spark/python:/spark/python/lib/py4j-0.10.1-src.zip
