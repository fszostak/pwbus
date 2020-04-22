# PWBus - _Engine Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

import string
import secrets
import re
import traceback

from pwbus.commons.logging import *

# _Engine
#
#


class _Engine():

    def __init__(self, name):
        self.name = name
        self.server_list = []

    # _Engine.run
    #
    def run(self, thread, channel_registry):
        start_threads = channel_registry["engine.start_threads"] \
            if channel_registry["engine.start_threads"] else 2
        running_threads = []
        alphabet = string.ascii_lowercase + string.digits
        try:
            for count in range(start_threads):
                hashcode = ''.join(secrets.choice(alphabet) for i in range(6))
                thread_name = f'{self.name}-{hashcode}-{str(count)}'

                log_debug(f'EngineManager.action - thread [{thread_name}]')
                self.server_list.append(
                    thread(
                        thread_id=count,
                        name=thread_name,
                        channel_registry=channel_registry
                    )
                )

            for created_thread in self.server_list:
                created_thread.start()
                running_threads.append(created_thread)

            return running_threads

        except KeyboardInterrupt:
            log_debug('_Engine.run - Please wait, stopping engines threads')

            for started_thread in running_threads:
                started_thread.do_run = False
                log_debug(
                    F'_Engine.run - Stopping thread [{started_thread.name}]')

            log_debug("_Engine.run - Interrupted by user")
            return None

        except:
            log_error(traceback, '_Engine.run - Error running engines threads')

    # _Engine.serve
    #
    def serve(self, channel_registry):
        raise NotImplementedError("_Engine.serve - Method need be implemented")
