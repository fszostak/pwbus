# PWBus - KafkaEngine Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Wed Dec 11 08:22:50 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.engines._engine import _Engine
from pwbus.engines.kafka.engine_thread import KafkaEngineThread


class KafkaEngine(_Engine):

    def __init__(self):
        super().__init__(
            name='kafka'
        )

    # KafkaEngine.serve
    #
    def serve(self, channel_registry):
        return self.run(KafkaEngineThread, channel_registry)
