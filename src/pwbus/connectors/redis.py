# PWBus - RedisConnector Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

import redis
import traceback

from pwbus.commons.logging import *
from pwbus.connectors._connector import _Connector

# RedisConnectorIn
#
#


class RedisConnector(_Connector):

    def __init__(self, host='redis', port=6379):
        super().__init__('redis', host=host, port=port)
        self.setConnectionPool(
            self.create_connection,
            max_size=100,
            max_usage=10000,
            idle=300,
            ttl=600
        )

    # RedisConnector.create_connection
    #
    def create_connection(self):
        try:
            return redis.Redis(
                host=self.host,
                port=self.port,
                db=0,
                password=None,
                socket_timeout=600
            )
        except:
            log_error(
                traceback, 'RedisConnector - Error on try connect to REDIS server')


# RedisConnectorIn
#
#
class RedisConnectorIn(RedisConnector):

    def __init__(self, host, port):
        super().__init__(host=host, port=port)

# RedisConnectorOut
#
#


class RedisConnectorOut(RedisConnector):

    def __init__(self, host, port):
        super().__init__(host=host, port=port)
