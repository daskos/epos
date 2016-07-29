from __future__ import absolute_import, division, print_function

import re
import json
from six import wraps
from toolz import curry
from collections import Iterator
from odo import resource, append, convert

from pykafka import KafkaClient


class Kafka(object):
    """ Parent class for data on Hadoop File System
    Examples
    --------
    >>> Kafka('topic', host='54.91.255.255', port=9092)  # doctest: +SKIP
    Alternatively use resource strings
    >>> resource('kafka://localhost:9092/path/to/file.csv')  # doctest: +SKIP
    """

    def __init__(self, topic, host='localhost', port=9092,
                 kafka=None, **kwargs):
        if kafka is not None:
            self.client = kafka
        elif host and port and topic:
            self.client = KafkaClient(hosts='{}:{}'.format(host, port))
        else:
            raise ValueError("No Kafka credentials found.\n"
                             "Supply keywords host=, port=, group=, channel=")
        self.topic = self.client.topics[topic]


simple_consumer_args = ('topic', 'cluster', 'consumer_group', 'partitions',
                        'fetch_message_max_bytes', 'num_consumer_fetchers',
                        'auto_commit_enable', 'auto_commit_interval_ms',
                        'queued_max_messages', 'fetch_min_bytes',
                        'fetch_wait_max_ms', 'offsets_channel_backoff_ms',
                        'offsets_commit_max_retries', 'auto_offset_reset',
                        'consumer_timeout_ms', 'auto_start',
                        'reset_offset_on_start', 'compacted_topic',
                        'generation_id', 'consumer_id')

producer_args = ('cluster', 'topic', 'partitioner', 'compression',
                 'max_retries', 'retry_backoff_ms', 'required_acks',
                 'ack_timeout_ms', 'max_queued_messages',
                 'min_queued_messages', 'linger_ms', 'block_on_queue_full',
                 'max_request_size', 'sync', 'delivery_reports',
                 'auto_start')


@curry
def filter_kwargs(fn, available_kwargs=()):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        d = {k: v for k, v in kwargs.items() if k in available_kwargs}
        return fn(*args, **d)

    return wrapper


simple_consumer = filter_kwargs(
    available_kwargs=simple_consumer_args+('dshape', 'loads', 'kafka'))

producer = filter_kwargs(
    available_kwargs=producer_args+('dshape', 'dumps', 'kafka'))


@resource.register('kafka://.*')
def resource_kafka(uri, kafka=None, **kwargs):
    pattern = r'kafka://(?P<host>[\w.-]*)?(:(?P<port>\d+))?/(?P<topic>[^\/]+)$'
    d = re.search(pattern, uri).groupdict()
    return Kafka(host=d['host'], port=d['port'], topic=d['topic'], kafka=kafka,
                 **kwargs)


@convert.register(Iterator, Kafka)
@simple_consumer
def kafka_to_iterator(dst, dshape=None, loads=json.loads, kafka=None, **kwargs):
    consumer = dst.topic.get_simple_consumer(**kwargs)
    for message in consumer:
        yield loads(message.value)


@append.register(Kafka, (list, Iterator))
@producer
def append_iterator_to_kafka(dst, src, dshape=None, dumps=json.dumps,
                             kafka=None, **kwargs):
    with dst.topic.get_producer(**kwargs) as producer:
        for item in src:
            producer.produce(dumps(item))
    return kafka


@append.register(Kafka, object)  # anything else
@producer
def append_object_to_kafka(dst, src, dshape=None, dumps=json.dumps,
                           kafka=None, **kwargs):
    with dst.topic.get_producer(**kwargs) as producer:
        producer.produce(dumps(src))
    return kafka
