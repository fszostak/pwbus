# PWBus - SqsClient Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sun Nov 17 20:10:24 -03 2019

import boto3
from json import dumps, loads, load
from time import sleep
import traceback

from pwbus.commons.logging import *
from pwbus.clients.client import Client
from pwbus.connectors.sqs import SqsConnector

# SqsClient
#
#


class SqsClient(SqsConnector, Client):

    def __init__(self, host=None, port=None):
        super().__init__()

    # SqsClient.get
    #
    def get(self, resource_name, correlation_id, wait=0.05, retries=20):

        log_debug(
            f'🔎 SqsClient.get - Retrieving response from [{resource_name}] with correlation_id [{correlation_id}] wait={wait}ms retries={retries}')
        try:
            with self.getConnectionPool().item() as connection:

                received_messages = connection.receive_message(
                    QueueUrl=resource_name,
                    MaxNumberOfMessages=5,
                    MessageAttributeNames=[
                        'All'
                    ],
                    WaitTimeSeconds=int(wait*retries),
                    VisibilityTimeout=1
                )

                if 'Messages' in received_messages:
                    for message in received_messages['Messages']:
                        if correlation_id == message['MessageAttributes'].get('Pwbus-Correlation-Id').get('StringValue'):
                            response = loads(message['Body'])

                            connection.delete_message(
                                QueueUrl=resource_name,
                                ReceiptHandle=message['ReceiptHandle']
                            )

                            log_debug(
                                f'SqsClient.get - Response found - correlation_id [{correlation_id}]]')
                            return self.clear_header(response)

                log_debug(
                    f'SqsClient.get - Response not found - correlation_id [{correlation_id}]')

        except:
            log_error(
                traceback, f'SqsClient.get - Failed to get message by correlation_id')

        return None
