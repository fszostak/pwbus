# PWBus - Registry Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

from json import loads
import traceback

from pwbus.commons.logging import *
from pwbus.connectors.redis import RedisConnector

# Registry. - Get registry channel settings
#
#


class Registry(RedisConnector):

    # Registry.getChannel
    #
    def getChannel(self, channel):
        try:
            with self.getConnectionPool().item() as connection:
                return loads(connection.get(f'wpbus.registry.{channel}'))
        except:
            log_debug(f'Registry.getChannel - Channel [{channel}] not found')
            return None
