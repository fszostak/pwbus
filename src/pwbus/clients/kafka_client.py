# PWBus - KafkaClient Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Wed Dec 11 07:13:13 -03 2019

from __future__ import division
import math
from itertools import islice
from pykafka.common import OffsetType
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable
from json import dumps, loads, load
from time import sleep
import traceback

from pwbus.commons.logging import *
from pwbus.clients.client import Client
from pwbus.connectors.kafka import KafkaConnector

# KafkaClient
#
#


class KafkaClient(KafkaConnector, Client):

    def __init__(self, host=None, port=None):
        super().__init__()

    # KafkaClient.get
    #
    def get(self, resource_name, correlation_id, wait=0.05, retries=20):

        log_debug(
            f'🔎 KafkaClient.get - Retrieving response from [{resource_name}] with correlation_id [{correlation_id}] wait={wait}ms retries={retries}')
        try:
            with self.getConnectionPool().item() as connection:
                topic = connection.topics[resource_name]

                LAST_N_MESSAGES = 50

                consumer = topic.get_simple_consumer(
                    consumer_group=resource_name,
                    queued_max_messages=LAST_N_MESSAGES,
                    consumer_timeout_ms=int(wait*retries*1000),
                    auto_commit_enable=True
                )

                offsets = [(p, op.next_offset - LAST_N_MESSAGES - 1)
                           for p, op in consumer._partitions.items()]
                offsets = [(p, o) if o != -1 else (p, -2) for p, o in offsets]
                consumer.reset_offsets(offsets)

                try:
                    consumer.consume()
                except (SocketDisconnectedError) as e:
                    log_error(traceback, "Kafka.get - consumer failed")
                    return None

                for message in consumer:
                    response = loads(''.join(message.value.decode('utf-8')))
                    if 'Pwbus-Correlation-Id' in response and \
                            correlation_id == response['Pwbus-Correlation-Id']:

                        partition = topic.partitions[0]
                        consumer.reset_offsets([(partition, message.offset)])
                        consumer.commit_offsets()

                        log_debug(
                            f'KafkaClient.get - Response found - correlation_id [{correlation_id}]]')
                        return self.clear_header(response)

                log_debug(
                    f'KafkaClient.get - Response not found - correlation_id [{correlation_id}]')

        except:
            log_error(
                traceback, f'KafkaClient.get - Failed to get message by correlation_id')

        return None
