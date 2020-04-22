# PWBus - Flow Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

import secrets
from pwbus.flows._message import _Message

# _Flow
#
#


class _Flow:

    ALLOWED_HEADERS = [
        "Pwbus-Channel",
        "Pwbus-Message-Id",
        "Pwbus-Correlation-Id",
        "Pwbus-Task-Id"
    ]

    def __init__(self, flow_type, connector):
        self.flow_type = flow_type
        self.connector = connector
        self.message = _Message()
        self.response = None
        self.registry = None
        self.running = True

    # _Flow.getType
    #
    def getType(self):
        return self.flow_type

    # _Flow.getConnector
    #
    def getConnector(self):
        return self.connector

    """ MESSAGE METHODS """

    # _Flow.getMessage
    #
    def getMessage(self):
        return self.message

    # _Flow.setMessage
    #
    def setMessage(self, message):
        self.message = message

    """ PAYLOAD METHODS """

    # _Flow.getPayloadField
    #
    def getPayloadField(self, name):
        return self.message.getPayloadField(name)

    # _Flow.setPayloadField
    #
    def setPayloadField(self, name, value):
        self.message.setPayloadField(name, value)

    """ HEADERS METHODS """

    # _Flow.getHeaders
    #
    def getHeaders(self):
        return self.message.getHeaders()

    # _Flow.setHeaders
    #
    def setHeaders(self, headers):
        self.message.setHeaders(headers)

    # _Flow.getHeadersEntry
    #
    def getHeadersEntry(self, name):
        return self.message.getHeadersEntry(name)

    # _Flow.setHeadersEntry
    #
    def setHeadersEntry(self, name, value):
        self.message.setHeadersEntry(name, value)

    """ RESPONSE METHODS """

    # _Flow.getResponse
    #
    def getResponse(self, field=None):
        if field:
            return self.response[field]
        else:
            return self.response

    # _Flow.setResponse
    #
    def setResponse(self, response):
        self.response = response

    # _Flow.setResponseEntry
    #
    def setResponseEntry(self, key, value):
        if key and value and not key.lower().startswith('pwbus_'):
            self.response[key] = value

    """ REGISTRY METHODS """

    # _Flow.getRegistry
    #
    def getRegistry(self):
        return self.registry

    # _Flow.setRegistry
    #
    def setRegistry(self, registry):
        self.registry = registry

    # _Flow.getSendTo
    #
    def getSendTo(self):
        return self.registry["flow.out.resource_name"]

    # _Flow.getRecvFrom
    #
    def getRecvFrom(self):
        return self.registry["flow.in.resource_name"]

    # _Flow.isPseudoSyncInEnabled
    #
    def isPseudoSyncInEnabled(self):
        return self.registry["flow.in.pseudosync_mode"] \
            if "flow.in.pseudosync_mode" in self.registry else True

    # _Flow.isPseudoSyncOutEnabled
    #
    def isPseudoSyncOutEnabled(self):
        return self.registry["flow.out.pseudosync_mode"] \
            if "flow.out.pseudosync_mode" in self.registry else True

    # _Flow.isTaskEnabled
    #
    def isTaskEnabled(self):
        return self.registry["flow.execute_tasks"] \
            if "flow.execute_tasks" in self.registry else True

    # def getWaitBackendResponse(self):
    #     if not self.wait_backend_response:
    #         self.wait_backend_response = \
    #             float(self.registry['flow.response_delay_ms']) \
    #             if 'flow.response_delay_ms' in self.registry else 0
    #     return self.wait_backend_response

    # _Flow.getTTL - message out expire time in seconds (default=10s)
    #
    def getTTL(self):
        return int(self.registry["flow.out.ttl_seconds"]
                   if self.registry["flow.out.ttl_seconds"] else 10)

    # _Flow.isDebugEnabled
    #
    def isDebugEnabled(self):
        return self.registry['engine.debug']

    # _Flow.getChannelName
    #
    def getChannelName(self):
        return self.registry['channel']

    # _Flow.stop
    #
    def stop(self):
        self.running = False

    # _Flow.stop
    #
    def isRunning(self):
        return self.running

# FlowIn
#
#


class FlowIn(_Flow):

    def __init__(self, flow_type, connector):
        super().__init__(flow_type, connector)

    # FlowIn.getPayloadFormat
    #
    def getPayloadFormat(self):
        return self.registry["flow.in.payload_format"]

    # FlowOut.getHost
    #
    def getHost(self):
        return self.registry["flow.in.host"] if "flow.in.host" in self.registry else None

    # FlowOut.getPort
    #
    def getPort(self):
        return self.registry["flow.in.port"] if "flow.in.port" in self.registry else None

    # FlowIn.prepareMessage
    #
    def prepareMessage(self, payload):
        # Move payload headers entries to Headers
        for field in list(payload):
            if field.startswith('Pwbus-'):
                self.setHeadersEntry(field, payload[field])
                del payload[field]

        # set payload
        message = self.getMessage()
        message.setPayloadFormat(self.registry["flow.in.payload_format"])
        message.setPayload(payload)

        headers = message.getHeaders()
        message.setMessageId(secrets.token_urlsafe())
        message.setHeadersEntry('Pwbus-Message-Id', message.getMessageId())
        if not 'Pwbus-Correlation-Id' in headers:
            message.setCorrelationId(secrets.token_urlsafe())
            message.setHeadersEntry(
                'Pwbus-Correlation-Id', message.getCorrelationId())
        task_id = headers['Pwbus-Task-Id'] \
            if 'Pwbus-Task-Id' in headers else None
        message.setTaskId(task_id)

        # set headers
        message.setHeadersEntry("Pwbus-Status-Code", 200)
        self.setMessage(message)

# FlowOut
#
#


class FlowOut(_Flow):

    def __init__(self, flow_type, connector):
        super().__init__(flow_type, connector)

    # FlowOut.getPayloadFormat
    #
    def getPayloadFormat(self):
        return self.registry["flow.out.payload_format"]

    # FlowOut.getHost
    #
    def getHost(self):
        return self.registry["flow.out.host"] if "flow.out.host" in self.registry else None

    # FlowOut.getPort
    #
    def getPort(self):
        return self.registry["flow.out.port"] if "flow.out.port" in self.registry else None

    # FlowOut.prepareMessage
    #
    def prepareMessage(self, message, headers):
        self.setMessage(message)
        self.setHeaders(headers)
        self.getMessage().setCorrelationId(headers['Pwbus-Correlation-Id'])
        self.getMessage().setMessageId(headers['Pwbus-Message-Id'])
        self.getMessage().setTaskId(
            headers['Pwbus-Task-Id'] if 'Pwbus-Task-Id' in headers else None
        )
