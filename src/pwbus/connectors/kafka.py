# PWBus - KafkaConnector Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Wed Dec 11 06:44:08 -03 2019

from pykafka import KafkaClient
import traceback

from pwbus.commons.logging import *
from pwbus.connectors._connector import _Connector

# KafkaConnectorIn
#
#


class KafkaConnector(_Connector):

    def __init__(self, host='kafka', port=9092):
        super().__init__('kafka', host, port)

        self.setConnectionPool(
            self.create_connection,
            max_size=10,
            max_usage=1000,
            idle=300,
            ttl=600
        )

    # KafkaConnector.create_connection
    #
    def create_connection(self):
        try:
            return KafkaClient(hosts=f'{self.host}:{self.port}')
        except:
            log_error(
                traceback, 'KafkaConnector - Error on try connect to KAFKA server')


# KafkaConnectorIn
#
#
class KafkaConnectorIn(KafkaConnector):

    def __init__(self, host, port):
        super().__init__(host=host, port=port)

# KafkaConnectorOut
#
#


class KafkaConnectorOut(KafkaConnector):

    def __init__(self, host, port):
        super().__init__(host=host, port=port)
