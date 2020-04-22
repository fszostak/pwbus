# PWBus - _Connector Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

import traceback
from connection_pool import ConnectionPool

from pwbus.commons.logging import *
from pwbus.connectors._connection_pool import _ConnectionPool

_connection_pool = _ConnectionPool()

# _Connector
#
#


class _Connector:

    def __init__(self, connector_type, host, port):
        self.connector_type = connector_type
        self.host = host if host else 'none'
        self.port = port if port else '0'
        self.connector_type_key = f'{self.connector_type}!!{self.host}:{str(self.port)}'
        self.connection_pool = _connection_pool

    def getType(self): return self.connector_type
    def getHost(self): return self.host
    def getPort(self): return self.port

    def getConnectionPool(self):
        if self.connection_pool.exists(self.connector_type_key):
            return self.connection_pool.get(self.connector_type_key)
        else:
            return None

    def setConnectionPool(self, create_connection, max_size, max_usage, idle, ttl):
        try:
            if not self.connection_pool.exists(self.connector_type_key):
                print(
                    f'🟡 _Connector.setConnectionPool - New connection pool [{self.connector_type_key}]')
                self.connection_pool.update(
                    connector_type=self.connector_type_key,
                    connection_pool=ConnectionPool(
                        create=create_connection,
                        max_size=max_size,
                        max_usage=max_usage,
                        idle=idle,
                        ttl=ttl
                    )
                )
        except:
            print('_Connector.setConnectionPool - Failed to set connection pool')
            traceback.print_exc()


# ConnectorIn
#
#
class ConnectorIn(_Connector):

    def __init__(self, connector_type, host, port):
        super().__init__(connector_type=connector_type, host=host, port=port)


# ConnectorOut
#
#
class ConnectorOut(_Connector):

    def __init__(self, connector_type, host, port):
        super().__init__(connector_type=connector_type, host=host, port=port)
