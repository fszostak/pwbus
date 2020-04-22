# PWBus - _EngineThread Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

import threading
import traceback

from pwbus.commons.logging import *
from pwbus.engines.engine_dispatch import EngineDispatch

# _EngineThread
#
#


class _EngineThread(threading.Thread):

    def __init__(self, thread_id, name, channel_registry):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.channel_registry = channel_registry
        self.channel = channel_registry["channel"]
        self.this_thread = threading.currentThread()

    # _EngineThread.stop
    #
    def stop(self):
        self.this_thread.do_run = False
        self.this_thread.join()

    # _EngineThread.isRunning
    #
    def isRunning(self):
        return getattr(self.this_thread, "do_run", True)

    # _EngineThread.dispatch
    #
    def dispatch(self, request=None):
        try:
            engine_dispatch = EngineDispatch()
            return engine_dispatch.route(self.channel_registry, request)
        except KeyboardInterrupt:
            log_debug(
                f'_EngineThread.run - Stopping thread for channel [{self.channel}]')
            self.stop()
            return None
        except:
            log_error(
                traceback, f'_EngineThread.run - Error thread execution for channel [{self.channel}]')

    # _EngineThread.run
    #

    def run(self):
        if not self.channel_registry:
            log_fatal(
                f'_EngineThread.run - channel [{self.channel}] not found')

        DEBUG = self.channel_registry['engine.debug']

        if DEBUG:
            log_debug(
                f'_EngineThread.run - Starting thread for channel [{self.channel}]')
            #log_debug_var(f'_EngineThread.run - channel [{self.channel}]', self.channel_registry)

        while True:
            try:
                if not self.isRunning():
                    if DEBUG:
                        log_debug(
                            f'_EngineThread.run - [{self.channel}] stopped ')
                    break

                response_flow = self.dispatch()

                if not response_flow:
                    log_debug('_EngineThread.run - Failed on dispatch message')

            except KeyboardInterrupt:
                self.stop()

            except:
                log_error(
                    traceback, '_EngineThread.run - Flow interrupted with errors')

        log_debug(f'_EngineThread.run - Stopping thread [{self.name}]')
