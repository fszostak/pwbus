# PWBus - KafkaFlowIn & KafkaFlowOut Classes
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Wed Dec 11 06:45:11 -03 2019

from pykafka.common import OffsetType
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable
import traceback

from pwbus.commons.logging import *
from pwbus.flows._flow import FlowIn, FlowOut
from pwbus.connectors.kafka import KafkaConnectorIn, KafkaConnectorOut

try:
    from pykafka.rdkafka import _rd_kafka  # noqa
    USE_RDKAFKA = True
    log_debug("USE_RDKAFKA Enabled!")
except ImportError:
    USE_RDKAFKA = False  # C extension not built
    log_debug("⚠️  USE_RDKAFKA Disabled!")

# KafkaFlowIn
#
#


class KafkaFlowIn(FlowIn):

    def __init__(self, request=None, host=None, port=None):
        super().__init__('in', KafkaConnectorIn(host=host, port=port))
        self.host = host
        self.port = port

    # KafkaFlowIn.receive
    #
    def receive(self, timeout=60):
        DEBUG = self.isDebugEnabled()
        resource_name = self.getRecvFrom()

        if DEBUG:
            log_debug(
                f'KafkaFlowIn.receive - Started to [{resource_name}] format={self.getPayloadFormat()}')

        try:
            if DEBUG:
                log_debug("KafkaFlowIn.receive.message - Getting connection")

            with self.getConnector().getConnectionPool().item() as connection:

                topic = connection.topics[resource_name]

                # pooling for wait messages
                while True:
                    consumer = topic.get_simple_consumer(
                        consumer_group=resource_name.encode(),
                        queued_max_messages=1,
                        consumer_timeout_ms=int(timeout*1000),
                        auto_commit_enable=True,
                        fetch_wait_max_ms=50
                    )
                    message = consumer.consume()
                    if message is not None:
                        payload_in = ''.join(message.value.decode('utf-8'))

                        consumer.commit_offsets()

                        if DEBUG:
                            log_debug(
                                f'↘️  KafkaFlowIn.receive.message - Message received successfully from [{resource_name}] - payload_in [{payload_in}]')
                        return payload_in

        except KeyboardInterrupt:
            log_debug('KafkaFlowIn.receive.message - Interrupted by user')
            self.stop()

        except:
            log_error(
                traceback, f'KafkaFlowIn.receive.message - Failed on receive message from [{self.getRecvFrom()}]')

        return None


# KafkaFlowOut
#
#
class KafkaFlowOut(FlowOut):

    def __init__(self, host=None, port=None):
        super().__init__('out', KafkaConnectorOut(host=host, port=port))
        self.host = host
        self.port = port

    # KafkaFlowOut.send
    #
    def send(self):
        DEBUG = self.isDebugEnabled()

        resource_name = self.getSendTo()
        payload_out = self.getMessage().getPayloadStream()

        if DEBUG:
            log_debug(
                f'KafkaFlowOut.send - Started to [{resource_name}] - payload_out [{payload_out}]')

        try:
            with self.getConnector().getConnectionPool().item() as connection:

                topic = connection.topics[resource_name]

                with topic.get_producer(
                        use_rdkafka=USE_RDKAFKA,
                        min_queued_messages=1,
                        linger_ms=0) as producer:
                    try:
                        producer.produce(payload_out)

                    except (SocketDisconnectedError, LeaderNotAvailable) as e:
                        print(e)
                        producer = topic.get_producer()
                        producer.stop()
                        producer.start()
                        producer.produce(payload_out)

                    if DEBUG:
                        log_debug(
                            f'↗️  KafkaFlowOut.send - Message sent successfully to [{resource_name}]')
                    return True
        except:
            log_error(
                traceback, f'KafkaFlowOut.send - Failed to send the message to [{resource_name}]')
            traceback.print_exc()

        return False
