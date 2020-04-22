# PWBus - RabbitmqConnector Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 30 15:01:58 -03 2019

import pika
from time import sleep
import traceback

from pwbus.commons.logging import *
from pwbus.connectors._connector import _Connector

# RabbitmqConnectorIn
#
#


class RabbitmqConnector(_Connector):

    def __init__(self, host='rabbitmq', port=5672):
        super().__init__('rabbitmq', host, port)

        self.setConnectionPool(
            self.create_connection,
            max_size=10,
            max_usage=1000,
            idle=300,
            ttl=600
        )

    # RabbitmqConnector.create_connection
    #
    def create_connection(self):
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            return pika.BlockingConnection(pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host='/',
                credentials=credentials,
                blocked_connection_timeout=60
            ))
        except:
            log_error(
                traceback, 'RabbitmqConnector - Error on try connect to RABBITMQ server. Waiting 5s too retry...')
            sleep(5)


# RabbitmqConnectorIn
#
#
class RabbitmqConnectorIn(RabbitmqConnector):

    def __init__(self, host, port):
        super().__init__(host=host, port=port)

# RabbitmqConnectorOut
#
#


class RabbitmqConnectorOut(RabbitmqConnector):

    def __init__(self, host, port):
        super().__init__(host=host, port=port)
