# PWBus - RabbitmqFlowIn & RabbitmqFlowOut Classes
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

import pika
import traceback

from pwbus.commons.logging import *
from pwbus.flows._flow import FlowIn, FlowOut
from pwbus.connectors.rabbitmq import RabbitmqConnectorIn, RabbitmqConnectorOut

# RabbitmqFlowIn
#
#


class RabbitmqFlowIn(FlowIn):

    def __init__(self, request=None, host=None, port=None):
        super().__init__('in', RabbitmqConnectorIn(host=host, port=port))
        self.host = host
        self.port = port
        self.payload_in = None
        self.mqchannel = None

    # RabbitmqFlowIn.receive
    #
    def receive(self, timeout=None):
        DEBUG = self.isDebugEnabled()
        resource_name = self.getRecvFrom()

        if DEBUG:
            log_debug(
                f'RabbitmqFlowIn.receive - Started to [{resource_name}] format={self.getPayloadFormat()}')

        try:
            if DEBUG:
                log_debug("RabbitmqFlowIn.receive.message - Getting connection")

            with self.getConnector().getConnectionPool().item() as connection:

                if not connection:
                    return None
                mqchannel = connection.channel()
                self.setMqchannel(mqchannel)

                mqchannel.queue_declare(queue=resource_name, durable=True, arguments={
                                        "x-queue-type": "classic"})
                mqchannel.basic_consume(
                    queue=resource_name,
                    auto_ack=True,
                    on_message_callback=self.receive_callback
                )

                if DEBUG:
                    log_debug(
                        f'⏱️  RabbitmqFlowIn.receive.message - Waiting for messages from [{resource_name}] ...')
                mqchannel.start_consuming()

                # convert rabbitmq payload from tuple type
                payload_in = self.getPayloadIn()

                if payload_in:
                    if DEBUG:
                        log_debug(
                            f'↘️  RabbitmqFlowIn.receive.message - Message received successfully from [{resource_name}] - payload_in [{payload_in}]')
                    return ''.join(payload_in.decode('utf-8'))

        except KeyboardInterrupt:
            log_debug('RabbitmqFlowIn.receive.message - Interrupted by user')
            self.stop()

        except:
            log_error(
                traceback, f'RabbitmqFlowIn.receive.message - Failed on receive message from [{self.getRecvFrom()}]')

        return None

    # RabbitmqFlowIn.setConnection
    #
    def setMqchannel(self, mqchannel):
        self.mqchannel = mqchannel

    # RabbitmqFlowIn.getConnection
    #
    def getMqchannel(self):
        return self.mqchannel

    # RabbitmqFlowIn.getPayloadIn
    #
    def getPayloadIn(self):
        return self.payload_in

    # RabbitmqFlowIn.setPayloadIn
    #
    def setPayloadIn(self, payload_in):
        self.payload_in = payload_in

    def receive_callback(self, ch, method, props, body):
        self.getMqchannel().stop_consuming()
        self.getMessage().setCorrelationId(props.correlation_id)
        self.setPayloadIn(body)


# RabbitmqFlowOut
#
#
class RabbitmqFlowOut(FlowOut):

    def __init__(self, host=None, port=None):
        super().__init__('out', RabbitmqConnectorOut(host=host, port=port))
        self.host = host
        self.port = port

    # RabbitmqFlowOut.send
    #
    def send(self):
        DEBUG = self.isDebugEnabled()

        resource_name = self.getSendTo()
        payload_out = self.getMessage().getPayloadAsString()

        if DEBUG:
            log_debug(
                f'RabbitmqFlowOut.send - Started to [{resource_name}] - payload_out [{payload_out}]')

        try:
            with self.getConnector().getConnectionPool().item() as connection:
                if not connection:
                    return False
                mqchannel = connection.channel()
                mqchannel.queue_declare(queue=resource_name, durable=True, arguments={
                                        "x-queue-type": "classic"})
                #reply_to=resource_name.replace('request', 'response'),
                mqchannel.basic_publish(
                    exchange='',
                    routing_key=resource_name,
                    properties=pika.BasicProperties(
                        correlation_id=self.getMessage().getCorrelationId()
                    ),
                    body=payload_out
                )

                log_debug(
                    f'↗️  RabbitmqFlowOut.send - Rabbitmq basic_publish to [{resource_name}] successfully')
                return True

        except:
            log_error(
                traceback, f'RabbitmqFlowOut.send - Failed to send the message to [{resource_name}]')
            traceback.print_exc()

        return False
