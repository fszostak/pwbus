# PWBus - RedisClient Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sun Nov 17 20:10:24 -03 2019

import redis
from json import dumps, loads, load
from time import sleep
import traceback

from pwbus.commons.logging import *
from pwbus.clients.client import Client
from pwbus.connectors.redis import RedisConnector

# RedisClient
#
#


class RedisClient(RedisConnector, Client):

    def __init__(self, host=None, port=None):
        super().__init__(host=host, port=port)

    # RedisClient.get
    #
    def get(self, resource_name, correlation_id, wait=0.05, retries=20):
        log_debug(
            f'🔎 RedisClient.get - Retrieving response from [{resource_name}] with correlation_id [{correlation_id}] wait={wait}ms retries={retries}')
        try:
            with self.getConnectionPool().item() as connection:
                response = None

                log_debug(
                    f'🔎 RedisClient.get - Connection retrieved from pool for [{resource_name}]')

                for retry in range(0, retries):

                    for scan_result in connection.sscan_iter(name=resource_name, match=f'*Pwbus-Correlation-Id*{correlation_id}*'):
                        response = scan_result.decode("utf-8")
                        log_debug(
                            f'🔎 RedisClient.get - Retrieve message from queue [{resource_name}]')
                        connection.srem(resource_name, response)
                        log_debug(
                            f'🔎 RedisClient.get - Put helper message for queue [{resource_name}]')
                        connection.rpop(
                            f'{resource_name}_helper'
                        )
                        response = loads(response)
                        log_debug(
                            f'RedisClient.get - Response found - correlation_id [{correlation_id}] retry [{retry+1}] wait [{wait}]')
                        return self.clear_header(response)

                    sleep(wait)

                log_debug(
                    f'RedisClient.get - Response not found - correlation_id [{correlation_id}]')

                return None

        except:
            log_error(
                traceback, f'RedisClient.get - Failed to get message by correlation_id')

        return None
