# PWBus - RedisEngine Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.engines._engine import _Engine
from pwbus.engines.redis.engine_thread import RedisEngineThread


class RedisEngine(_Engine):

    def __init__(self):
        super().__init__(
            name='redis'
        )

    # RedisEngine.serve
    #
    def serve(self, channel_registry):
        return self.run(RedisEngineThread, channel_registry)
