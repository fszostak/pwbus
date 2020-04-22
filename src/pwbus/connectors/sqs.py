# PWBus - SqsConnector Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Dec  7 09:46:09 -03 2019

import boto3
import traceback

from pwbus.commons.logging import *
from pwbus.connectors._connector import _Connector

# SqsConnectorIn
#
#


class SqsConnector(_Connector):

    def __init__(self, host=None, port=None):
        super().__init__('sqs', host, port)
        self.setConnectionPool(
            self.create_connection,
            max_size=10,
            max_usage=1000,
            idle=300,
            ttl=600
        )

    # SqsConnector.create_connection
    #
    def create_connection(self):
        try:
            return boto3.client('sqs')
        except:
            log_error(
                traceback, 'SqsConnector - Error on try connect to SQS server')


# SqsConnectorIn
#
#
class SqsConnectorIn(SqsConnector):

    def __init__(self, host, port):
        super().__init__(host=host, port=port)

# SqsConnectorOut
#
#


class SqsConnectorOut(SqsConnector):

    def __init__(self, host, port):
        super().__init__(host=host, port=port)
