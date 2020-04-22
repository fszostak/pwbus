# PWBus - MongoEngineThread Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Dec 14 15:32:34 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.engines._engine_thread import _EngineThread


class MongoEngineThread(_EngineThread):

    def __init__(self, thread_id, name, channel_registry):
        super().__init__(thread_id, name, channel_registry)
