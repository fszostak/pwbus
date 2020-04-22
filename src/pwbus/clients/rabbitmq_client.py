# PWBus - RabbitmqClient Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sun Nov 17 20:10:24 -03 2019

import pika
from json import dumps, loads, load
from time import sleep
import traceback

from pwbus.commons.logging import *
from pwbus.clients.client import Client
from pwbus.connectors.rabbitmq import RabbitmqConnector

# RabbitmqClient
#
#


class RabbitmqClient(RabbitmqConnector, Client):

    def __init__(self, host=None, port=None):
        super().__init__(host=host, port=port)
        self.response = None
        self.correlation_id = None

    def setCorrelationId(self, correlation_id):
        self.correlation_id = correlation_id

    def onResponse(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            self.response = body

    # RabbitmqClient.get
    #
    def get(self, resource_name, correlation_id, wait=0.05, retries=20):
        self.setCorrelationId(correlation_id)
        log_debug(
            f'🔎 RabbitmqClient.get - Retrieving response from [{resource_name}] with correlation_id [{correlation_id}] wait={wait}ms retries={retries}')
        try:
            with self.getConnectionPool().item() as connection:
                channel = connection.channel()
                channel.basic_consume(
                    queue=resource_name,
                    on_message_callback=self.onResponse,
                    auto_ack=True
                )

                retry = 0
                while self.response is None and retry < retries:
                    connection.process_data_events()
                    sleep(wait)
                    retry += 1

                channel.stop_consuming()

                if self.response is None:
                    log_debug(
                        f'RabbitmqClient.get - Response not found - correlation_id [{correlation_id}]')
                else:
                    self.response = loads(self.response)
                    log_debug(
                        f'RabbitmqClient.get - Response found - correlation_id [{correlation_id}] retry [{retry+1}]')

                return self.clear_header(self.response)

        except:
            log_error(
                traceback, f'RabbitmqClient.get - Failed to get message by correlation_id')

        return None
