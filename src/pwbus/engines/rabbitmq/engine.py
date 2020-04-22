# PWBus - RabbitmqEngine Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 30 15:08:11 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.engines._engine import _Engine
from pwbus.engines.rabbitmq.engine_thread import RabbitmqEngineThread


class RabbitmqEngine(_Engine):

    def __init__(self):
        super().__init__(
            name='rabbitmq'
        )

    # RabbitmqEngine.serve
    #
    def serve(self, channel_registry):
        return self.run(RabbitmqEngineThread, channel_registry)
