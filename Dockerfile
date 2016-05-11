FROM lensa/pyspark:pip-2-3-spark-1.6.1-hdfs-client-cdh5.7-java-8-mesos-0.28.0-2.0.16-ubuntu-15.04

RUN apt-get update && apt-get install -y libssl-dev
RUN pip install cloudpickle && \
  wget http://downloads.mesosphere.io/master/debian/8/mesos-0.27.0-py2.7-linux-x86_64.egg -O mesos.egg && \
  easy_install mesos.egg && \
  rm /mesos.egg

ADD . /epos
WORKDIR /epos
RUN pip install .[complete]
