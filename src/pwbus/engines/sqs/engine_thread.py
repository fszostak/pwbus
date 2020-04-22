# PWBus - SqsEngineThread Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Dec  7 09:46:09 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.engines._engine_thread import _EngineThread


class SqsEngineThread(_EngineThread):

    def __init__(self, thread_id, name, channel_registry):
        super().__init__(thread_id, name, channel_registry)
