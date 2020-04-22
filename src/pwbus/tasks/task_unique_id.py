### PWBus - TaskUniqueId
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sun Dec 15 10:36:07 -03 2019

from pwbus.commons.logging import *
from pwbus.connectors.redis import RedisConnector


class TaskUniqueId(RedisConnector):

    # TaskUniqueId.get
    #
    def get(self):
        try:
            with self.getConnectionPool().item() as connection:
                return connection.incr("pwbus_unique", amount=1)
        except:
            log_error("TaskUniqueId.get - Failed on get unique number")
