# PWBus - RegistryLoader Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Tue Nov 19 21:05:19 -03 2019

from os import getenv
import redis
from json import dumps, load
import traceback

from pwbus.commons.logging import *
from pwbus.connectors.redis import RedisConnector

# Registry loader
#
#


class RegistryLoader(RedisConnector):
    def __init__(self, registry_file):
        RedisConnector.__init__(self)
        self.registry_file = registry_file

    # RegistryLoader.load
    #
    def load(self):
        try:
            with open(self.registry_file) as json_file:
                registry_list = load(json_file)

            with self.getConnectionPool().item() as connection:
                for item in registry_list:
                    connection.delete(f'wpbus.registry.{item["channel"]}')
                    connection.set(
                        f'wpbus.registry.{item["channel"]}', dumps(item))

            log_debug('PWBUS Registry - Loaded successfully')

        except:
            log_error(
                traceback, 'RegistryLoader.load - Error trying to load the registry')
            log_exit('RegistryLoader.load - exited')

    # RegistryLoader.getRunnableEngines
    #
    def getRunnableEngines(self):
        engines = []

        try:
            with open(self.registry_file) as json_file:
                registry_list = load(json_file)

            for channel_entry in registry_list:
                if channel_entry["engine.enabled"]:
                    if channel_entry["flow.in.resource_type"] != 'http' and \
                            not channel_entry["flow.in.resource_type"] in engines:
                        engines.append(channel_entry["flow.in.resource_type"])

            log_debug(f'Registry.getRunnableEngines - Engines {engines}')
            return engines

        except:
            log_error(
                traceback, 'Registry.getRunnableEngines - Error getting engines')
            log_exit('RegistryLoader.getRunnableEngines - exited')

    # RegistryLoader.getRunnableChannels
    #
    def getRunnableChannels(self, engine):
        channels = []

        try:
            with open(self.registry_file) as json_file:
                registry_list = load(json_file)

            for channel_entry in registry_list:
                if channel_entry["flow.in.resource_type"] == engine:
                    if not channel_entry['channel'] in channels:
                        channels.append(channel_entry['channel'])

            log_debug(f'Registry.getRunnableChannels - Channels {channels}')
            return channels

        except:
            log_error(
                traceback, 'Registry.getRunnableChannels - Error getting channels')
            log_exit('RegistryLoader.getRunnableChannels - exited')
