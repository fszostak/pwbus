# PWBus - SqsEngine Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Dec  7 09:46:09 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.engines._engine import _Engine
from pwbus.engines.sqs.engine_thread import SqsEngineThread


class SqsEngine(_Engine):

    def __init__(self):
        super().__init__(
            name='sqs'
        )

    # SqsEngine.serve
    #
    def serve(self, channel_registry):
        return self.run(SqsEngineThread, channel_registry)
