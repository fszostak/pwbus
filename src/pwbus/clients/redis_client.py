# PWBus - RedisClient Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sun Nov 17 20:10:24 -03 2019

import redis
from json import dumps, loads, load
from time import sleep
from datetime import datetime
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
    def get(self, resource_name, correlation_id, wait=0.10, retries=3):
        log_debug(
            f'🔎 RedisClient.get - Retrieving response from [{resource_name}] with correlation_id [{correlation_id}] wait={wait}ms retries={retries}')
        try:
            with self.getConnectionPool().item() as connection:
                response = None

                log_debug(
                    f'🔎 RedisClient.get - Connection retrieved from pool for [{resource_name}]')

                for retry in range(0, retries):

                    start = datetime.now()

                    for scan_result in connection.sscan_iter(name=resource_name, match=f'*Pwbus-Correlation-Id*{correlation_id}*'):
                        end = datetime.now()
                        response = scan_result.decode("utf-8")
                        connection.srem(resource_name, response)
                        connection.rpop(
                            f'{resource_name}_helper'
                        )
                        response = loads(response)
                        log_debug(
                            f'RedisClient.get - Response found - correlation_id [{correlation_id}] retry [{retry+1}] wait [{wait}] scan-time [{end - start}ms]')
                        return self.clear_header(response)

                    end = datetime.now()
                    log_debug(
                        f'RedisClient.get - Waiting message - correlation_id [{correlation_id}] retry [{retry+1}] wait [{wait}] scan-time [{end - start}ms]')

                    sleep(wait)

                log_debug(
                    f'RedisClient.get - Response not found - correlation_id [{correlation_id}]')

                return None

        except:
            log_error(
                traceback, f'RedisClient.get - Failed to get message by correlation_id')

        return None
