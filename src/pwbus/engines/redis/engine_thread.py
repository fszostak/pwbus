# PWBus - RedisEngineThread Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.engines._engine_thread import _EngineThread


class RedisEngineThread(_EngineThread):

    def __init__(self, thread_id, name, channel_registry):
        super().__init__(thread_id, name, channel_registry)
