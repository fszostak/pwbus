# PWBus - EngineManager Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Tue Nov 19 20:07:35 -03 2019

import sys
import importlib
import traceback

from pwbus.commons.logging import *
from pwbus.engines.engine_registry_loader import RegistryLoader
from pwbus.engines.engine_registry import Registry
from pwbus.engines.engine_monitor import EngineMonitor

# EngineManager
#
#


class EngineManager():

    def __init__(self, registry_file):
        # Load registry entries
        self.registry_loader = RegistryLoader(registry_file)
        self.registry_loader.load()

    # EngineManager.action
    #
    def action(self, command, engine, channel):
        running_threads = []

        if not engine:
            engines = self.registry_loader.getRunnableEngines()
        else:
            engines = [engine]

        if engines == []:
            log_debug(
                'EngineManager.action - All engines are disabled, check engine.enabled property in pwbus_registry.json')
            return

        # Start pwbus_monitor
        EngineMonitor().start()

        # Running engines threads
        try:
            for engine_entry in engines:
                module_name = f'pwbus.engines.{engine_entry}.engine'
                module = importlib.import_module(module_name)
                class_ = getattr(module, f'{engine_entry.capitalize()}Engine')

                # channel is None run all channels else only the channel specified
                if not channel:
                    runnable_channels = self.registry_loader.getRunnableChannels(
                        engine=engine_entry)
                else:
                    runnable_channels = [channel]

                registry = Registry()

                log_debug(
                    f'\n\n=====({engine_entry} engines startup)===============================\n')
                for channel_entry in runnable_channels:
                    channel_registry = registry.getChannel(channel_entry)
                    if channel_registry["engine.enabled"]:
                        if not channel_registry["engine.start_threads"]:
                            channel_registry["engine.start_threads"] = 2

                        engine_instance = class_()
                        log_debug(
                            f'EngineManager.action - Starting threads for engine=[{engine_entry}] class=[{engine_entry.capitalize()}Engine] channel=[{channel_entry}]')
                        running_threads += engine_instance.serve(
                            channel_registry)

            # for started_thread in running_threads:
            #   started_thread.join()

        except KeyboardInterrupt:
            log_debug(
                'EngineManager.action - Please wait, stopping engines threads')

            for started_thread in running_threads:
                started_thread.do_run = False
                log_debug(
                    f'EngineManager.action - Stopping thread [{started_thread.name}]')

            log_fatal("EngineManager.action - Interrupted by user")

        except:
            log_error(
                traceback, 'EngineManager.action - Error starting engines threads')
            log_fatal("EngineManager.action - stopped")
