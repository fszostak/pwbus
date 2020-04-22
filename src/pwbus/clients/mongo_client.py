# PWBus - MongoClient Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sun Nov 17 20:10:24 -03 2019

import boto3
from time import sleep
import traceback

from pwbus.commons.logging import *
from pwbus.clients.client import Client
from pwbus.connectors.mongo import MongoConnector

# MongoClient
#
#


class MongoClient(MongoConnector, Client):

    def __init__(self, host=None, port=None):
        super().__init__()

    # MongoClient.get
    #
    def get(self, resource_name, correlation_id, wait=0.05, retries=20):
        resource_name = resource_name.split('.')
        database = resource_name[0]
        collection = resource_name[1]
        log_debug(
            f'🔎 MongoClient.get - Retrieving response from [{resource_name}] with correlation_id [{correlation_id}] wait={wait}ms retries={retries}')
        try:
            with self.getConnectionPool().item() as connection:

                db = connection[database]
                retry_count = 0

                # pooling for wait messages
                while retry_count < retries:
                    response = db[collection].find_one_and_delete(
                        {"Pwbus-Correlation-Id": correlation_id},
                        {"_id": 0}
                    )

                    if response:
                        log_debug(
                            f'MongoClient.get - Response found - correlation_id [{correlation_id}]]')
                        return self.clear_header(response)
                    else:
                        sleep(wait)
                        retry_count += 1

                log_debug(
                    f'MongoClient.get - Response not found - correlation_id [{correlation_id}]')
                return None

        except:
            log_error(
                traceback, f'MongoClient.get - Failed to get message by correlation_id')

        return None
