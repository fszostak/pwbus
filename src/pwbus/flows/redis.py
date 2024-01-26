# PWBus - RedisFlowIn & RedisFlowOut Classes
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

from time import sleep
import traceback

from pwbus.commons.logging import *
from pwbus.flows._flow import FlowIn, FlowOut
from pwbus.connectors.redis import RedisConnectorIn, RedisConnectorOut

# RedisFlowIn
#
#


class RedisFlowIn(FlowIn):

    def __init__(self, request=None, host=None, port=None):
        super().__init__('in', RedisConnectorIn(host=host, port=port))
        self.host = host
        self.port = port

    # RedisFlowIn.receive
    #
    def receive(self, timeout=300):
        DEBUG = self.isDebugEnabled()
        resource_name = self.getRecvFrom()

        if DEBUG:
            log_debug(
                f'RedisFlowIn.receive - Started to [{resource_name}] format={self.getPayloadFormat()}')

        while True:
            try:
                if DEBUG:
                    log_debug(
                        "RedisFlowIn.receive.message - Getting connection")

                with self.getConnector().getConnectionPool().item() as connection:

                    # pooling for wait messages
                    while True:

                        # for pseudosync tasks, with SADD, SPOP and SSCAN (client)
                        if self.isPseudoSyncInEnabled():

                            while True:

                                if DEBUG:
                                    log_debug(
                                        f'⏱️  RedisFlowIn.receive.message - Waiting for messages from [{resource_name}|PSEUDOSYNC] ...')

                                # pattern event notification - for SADD command
                                # (https://redis.io/commands/blpop#pattern-e
                                tuple_in = connection.spop(resource_name)

                                if tuple_in:

                                    # convert redis payload from tuple type
                                    payload_in = ''.join(
                                        tuple_in.decode('utf-8'))
                                    if DEBUG:
                                        log_debug(
                                            f'↘️  RedisFlowIn.receive.message - Message received successfully from [{resource_name}|PSEUDOSYNC] - payload_in [{payload_in}]')
                                    return payload_in

                                connection.brpop(
                                    f'{resource_name}.helper', timeout)

                        # for async tasks, with LPUSH and BRPOPLPUSH
                        else:
                            if DEBUG:
                                log_debug(
                                    f'⏱️  RedisFlowIn.receive.message - Waiting for messages from [{resource_name}] ...')
                            tuple_in = connection.brpoplpush(
                                resource_name,
                                name + '_reliable',
                                timeout
                            )

                            if tuple_in:
                                # convert redis payload from tuple type
                                payload_in = ''.join(tuple_in.decode('utf-8'))
                                if DEBUG:
                                    log_debug(
                                        f'↘️  RedisFlowIn.receive.message - Message received successfully from [{resource_name}] - payload_in [{payload_in}]')
                                return payload_in

            except KeyboardInterrupt:
                log_debug('RedisFlowIn.receive.message - Interrupted by user')
                self.stop()

            except:
                log_error(
                    traceback, f'RedisFlowIn.receive.message - Failed on receive message from [{self.getRecvFrom()}]')
                sleep(5)


# RedisFlowOut
#
#
class RedisFlowOut(FlowOut):

    def __init__(self, host=None, port=None):
        super().__init__('out', RedisConnectorOut(host=host, port=port))
        self.host = host
        self.port = port

    # RedisFlowOut.send
    #
    def send(self):
        DEBUG = self.isDebugEnabled()

        resource_name = self.getSendTo()
        payload_out = self.getMessage().getPayloadAsString()

        if DEBUG:
            log_debug(
                f'RedisFlowOut.send - Started to [{resource_name}] - payload_out [{payload_out}]')

        try:
            with self.getConnector().getConnectionPool().item() as connection:

                if self.isPseudoSyncOutEnabled():
                    position = connection.sadd(resource_name, payload_out)
                    if position:
                        if DEBUG:
                            log_debug(
                                f'↗️  RedisFlowOut.send - Redis SADD in position [{position}]')

                        # pattern event notification
                        # (https://redis.io/commands/blpop#pattern-event-notification)
                        connection.lpush(f'{resource_name}.helper', '1')
                        return True
                    else:
                        log_debug(
                            f'RedisFlowOut.send - Redis SADD failed, payload already exists in [{resource_name}]')
                else:
                    position = connection.lpush(resource_name, payload_out)

                    if position:
                        if DEBUG:
                            log_debug(
                                f'↗️  RedisFlowOut.send - Redis LPUSH in position [{position}]')
                        self.setResponseEntry("queue_position", position)
                        return True
                    else:
                        log_debug(
                            f'RedisFlowOut.send - Redis LPUSH failed to [{resource_name}]')
                        return False

        except:
            log_error(
                traceback, f'RedisFlowOut.send - Failed to send the message to [{resource_name}]')

        return False
