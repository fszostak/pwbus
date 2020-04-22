# PWBus - KafkaEngineThread Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Wed Dec 11 08:22:50 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.engines._engine_thread import _EngineThread


class KafkaEngineThread(_EngineThread):

    def __init__(self, thread_id, name, channel_registry):
        super().__init__(thread_id, name, channel_registry)
