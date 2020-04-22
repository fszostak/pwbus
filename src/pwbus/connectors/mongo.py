# PWBus - MongoConnector Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Dec 14 11:32:03 -03 2019

import pymongo
import traceback

from pwbus.commons.logging import *
from pwbus.connectors._connector import _Connector

# MongoConnectorIn
#
#


class MongoConnector(_Connector):

    def __init__(self, host='mongodb', port=21017):
        super().__init__('mongo', host, port)
        self.setConnectionPool(
            self.create_connection,
            max_size=10,
            max_usage=1000,
            idle=300,
            ttl=600
        )

    # MongoConnector.create_connection
    #
    def create_connection(self):
        try:
            return pymongo.MongoClient(f'mongodb://root:example@{self.getHost()}/')
        except:
            log_error(
                traceback, 'MongoConnector - Error on try connect to mongodb server')


# MongoConnectorIn
#
#
class MongoConnectorIn(MongoConnector):

    def __init__(self, host, port):
        super().__init__(host=host, port=port)

# MongoConnectorOut
#
#


class MongoConnectorOut(MongoConnector):

    def __init__(self, host, port):
        super().__init__(host=host, port=port)
