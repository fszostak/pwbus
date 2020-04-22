# PWBus - HttpConnectorIn and HttpConnectorOut Classes
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

from pwbus.connectors._connector import ConnectorIn, ConnectorOut

# HttpConnectorIn
#
#


class HttpConnectorIn(ConnectorIn):

    def __init__(self, host=None, port=None):
        super().__init__(connector_type='http-in', host=host, port=port)


# HttpConnectorOut
#
#
class HttpConnectorOut(ConnectorOut):

    def __init__(self, host=None, port=None):
        super().__init__(connector_type='http-out', host=host, port=port)
