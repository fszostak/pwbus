# PWBus - SqsFlowIn & SqsFlowOut Classes
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Dec  7 09:46:09 -03 2019

from time import sleep
import traceback

from pwbus.commons.logging import *
from pwbus.flows._flow import FlowIn, FlowOut
from pwbus.connectors.sqs import SqsConnectorIn, SqsConnectorOut

# SqsFlowIn
#
#


class SqsFlowIn(FlowIn):

    def __init__(self, request=None, host=None, port=None):
        super().__init__('in', SqsConnectorIn(host=host, port=port))
        self.host = host
        self.port = port

    # SqsFlowIn.receive
    #
    def receive(self, timeout=20):
        DEBUG = self.isDebugEnabled()
        resource_name = self.getRecvFrom()

        if DEBUG:
            log_debug(
                f'SqsFlowIn.receive - Started to [{resource_name}] format={self.getPayloadFormat()}')

        while True:
            try:
                if DEBUG:
                    log_debug("SqsFlowIn.receive.message - Getting connection")

                with self.getConnector().getConnectionPool().item() as connection:

                    # pooling for wait messages
                    while True:

                        if DEBUG:
                            log_debug(
                                f'⏱️  SqsFlowIn.receive.message - Waiting for messages from [{resource_name}|PSEUDOSYNC] ...')

                        # Long poll for message on provided SQS queue
                        received_messages = connection.receive_message(
                            QueueUrl=resource_name,
                            AttributeNames=[
                                'SentTimestamp'
                            ],
                            MaxNumberOfMessages=1,
                            MessageAttributeNames=[
                                'All'
                            ],
                            WaitTimeSeconds=timeout,
                            VisibilityTimeout=5
                        )

                        if 'Messages' in received_messages:
                            message = received_messages['Messages'][0]
                            payload_in = message['Body']

                            connection.delete_message(
                                QueueUrl=resource_name,
                                ReceiptHandle=message['ReceiptHandle']
                            )

                            if DEBUG:
                                log_debug(
                                    f'↘️  SqsFlowIn.receive.message - Message received successfully from [{resource_name}] - payload_in [{payload_in}]')
                            return payload_in

            except KeyboardInterrupt:
                log_debug('SqsFlowIn.receive.message - Interrupted by user')
                self.stop()

            except:
                log_error(
                    traceback, f'SqsFlowIn.receive.message - Failed on receive message from [{self.getRecvFrom()}]')
                sleep(5)

# SqsFlowOut
#
#


class SqsFlowOut(FlowOut):

    def __init__(self, host=None, port=None):
        super().__init__('out', SqsConnectorOut(host=host, port=port))
        self.host = host
        self.port = port

    # SqsFlowOut.send
    #
    def send(self):
        DEBUG = self.isDebugEnabled()

        resource_name = self.getSendTo()
        payload_out = self.getMessage().getPayloadAsString()

        if DEBUG:
            log_debug(
                f'SqsFlowOut.send - Started to [{resource_name}] - payload_out [{payload_out}]')

        try:
            with self.getConnector().getConnectionPool().item() as connection:

                # Send message to SQS queue
                response = connection.send_message(
                    QueueUrl=resource_name,
                    DelaySeconds=0,
                    MessageAttributes={
                        'Author': {
                            'DataType': 'String',
                            'StringValue': 'Pwbus-Engine-v1'
                        },
                        'Pwbus-Correlation-Id': {
                            'DataType': 'String',
                            'StringValue': self.getMessage().getCorrelationId()
                        }
                    },
                    MessageBody=(
                        payload_out
                    )
                )

                # when fifo
                # MessageGroupId='1'
                # MessageDeduplicationId=self.getMessage().getMessageId()

                if DEBUG:
                    log_debug(
                        f'↗️  SqsFlowOut.send - Sqs MessageId [{response["MessageId"]}]')
                return True
        except:
            log_error(
                traceback, f'SqsFlowOut.send - Failed to send the message to [{resource_name}]')
            traceback.print_exc()

        return False
