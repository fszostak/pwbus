# PWBus - MongoEngine Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Dec 14 15:32:34 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.engines._engine import _Engine
from pwbus.engines.mongo.engine_thread import MongoEngineThread


class MongoEngine(_Engine):

    def __init__(self):
        super().__init__(
            name='mongo'
        )

    # MongoEngine.serve
    #
    def serve(self, channel_registry):
        return self.run(MongoEngineThread, channel_registry)
