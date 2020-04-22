# PWBus - MongoFlowIn & MongoFlowOut Classes
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Dec 14 11:32:03 -03 2019

from bson import Binary, Code, ObjectId
from bson.json_util import dumps
from time import sleep
import traceback

from pwbus.commons.logging import *
from pwbus.flows._flow import FlowIn, FlowOut
from pwbus.connectors.mongo import MongoConnectorIn, MongoConnectorOut


# MongoFlowIn
#
#
class MongoFlowIn(FlowIn):

    def __init__(self, request=None, host=None, port=None):
        super().__init__('in', MongoConnectorIn(host=host, port=port))
        self.host = host
        self.port = port

    # MongoFlowIn.receive
    #
    def receive(self, timeout=60):
        DEBUG = self.isDebugEnabled()
        resource_name = self.getRecvFrom().split('.')
        try:
            database = resource_name[0]
            collection = resource_name[1]
        except:
            log_debug(
                f'🟥 MongoFlowIn.receive - Invalid resource name [{self.getRecvFrom()}] specify "database.collection"')
            return None

        if DEBUG:
            log_debug(
                f'MongoFlowIn.receive - Started to [{resource_name}] format={self.getPayloadFormat()}')

        while True:
            try:
                if DEBUG:
                    log_debug(
                        "MongoFlowIn.receive.message - Getting connection")

                with self.getConnector().getConnectionPool().item() as connection:

                    db = connection[database]

                    while True:
                        if collection in db.list_collection_names():
                            break
                        log_debug(
                            f'MongoFlowIn.receive.message - Collection [{collection}] not exists in database [{database}] - Waiting ... (30s)')
                        sleep(30)

                    wait = 0

                    # pooling for wait messages
                    while True:

                        if wait <= 0:
                            wait = timeout
                            if DEBUG:
                                log_debug(
                                    f'⏱️  MongoFlowIn.receive.message - Waiting for messages from [{resource_name}|PSEUDOSYNC] ...')

                        response = db[collection].find_one_and_delete({}, {
                                                                      "_id": 0})

                        if response:
                            payload_in = dumps(response)
                            if DEBUG:
                                log_debug(
                                    f'↘️  MongoFlowIn.receive.message - Message received successfully from [{resource_name}] - payload_in [{payload_in}]')
                            return payload_in

                        sleep(0.100)
                        wait -= 0.100

            except KeyboardInterrupt:
                log_debug('MongoFlowIn.receive.message - Interrupted by user')
                self.stop()

            except:
                log_error(
                    traceback, f'MongoFlowIn.receive.message - Failed on receive message from [{self.getRecvFrom()}]')

            return None


# MongoFlowOut
#
#
class MongoFlowOut(FlowOut):

    def __init__(self, host=None, port=None):
        super().__init__('out', MongoConnectorOut(host=host, port=port))
        self.host = host
        self.port = port

    # MongoFlowOut.send
    #
    def send(self):
        DEBUG = self.isDebugEnabled()

        resource_name = self.getSendTo().split('.')
        payload_out = self.getMessage().getPayload()
        try:
            database = resource_name[0]
            collection = resource_name[1]
        except:
            log_debug(
                f'🟥 MongoFlowOut.send - Invalid resource name [{self.getSendTo()}] specify "database.collection"')
            return False

        if DEBUG:
            log_debug(
                f'MongoFlowOut.send - Started to [{resource_name}] - payload_out [{payload_out}]')

        try:
            with self.getConnector().getConnectionPool().item() as connection:

                db = connection[database]
                col = db[collection]

                if col.insert(payload_out):
                    if DEBUG:
                        log_debug(
                            f'↗️  MongoFlowOut.send - Record inserted into mongodb')
                    return True
        except:
            log_error(
                traceback, f'MongoFlowOut.send - Failed to send the message to [{resource_name}]')
            traceback.print_exc()

        return False
