# PWBus - Message Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

import json
import traceback

from pwbus.commons.logging import *

# _Message
#
#


class _Message:

    PAYLOAD_TYPES = [
        {"type": "text",    "content-type": "text/plain"},
        {"type": "json",    "content-type": "application/json"},
        {"type": "xml",     "content-type": "application/xml"},
        {"type": "iso8583", "content-type": "text/plain"},
        {"type": "csv",     "content-type": "text/csv"},
    ]

    def __init__(self):
        self.payload_format = "json"
        self.task_id = None
        self.message_id = None
        self.correlation_id = None
        self.task_id = None
        self.headers = dict()
        self.payload = dict()
        self.payloadAsString = None

    # _Message.setPayloadFormat
    #
    def setPayloadFormat(self, payload_format):
        self.payload_format = payload_format

    # _Message.getMessageId
    #
    def getMessageId(self):
        return self.message_id

    # _Message.setMessageId
    #
    def setMessageId(self, message_id):
        self.message_id = message_id

    # _Message.getCorrelationId
    #
    def getCorrelationId(self):
        return self.correlation_id

    # _Message.setCorrelationId
    #
    def setCorrelationId(self, correlation_id):
        self.correlation_id = correlation_id

    # _Message.getTaskId
    #
    def getTaskId(self):
        return self.task_id

    # _Message.setTaskId
    #
    def setTaskId(self, task_id):
        self.task_id = task_id

    # _Message.getPayload
    #
    def getPayload(self):
        return self.payload

    # _Message.getHeaders
    #
    def getHeaders(self):
        return self.headers

    # _Message.setHeaders
    #
    def setHeaders(self, headers):
        self.headers = headers

    # _Message.setHeadersEntry
    #
    def setHeadersEntry(self, name, value):
        self.headers[name] = value

    # _Message.getHeadersEntry
    #
    def getHeadersEntry(self, name):
        return self.headers[name] if name in self.headers else None

    # _Message.setPayload
    #
    def setPayload(self, payload, create_correlation_id=False, keep_correlation_id=True):
        if not isinstance(payload, dict):
            log_debug(
                "_Message.setPayload - payload is invalid, need be 'dict' type")
            return None
        self.payload = payload

    # _Message.setPayloadField
    #
    def setPayloadField(self, name, value):
        self.payload[name] = value

    # _Message.getPayloadStream
    #
    def getPayloadStream(self):
        return self.getPayloadAsString().encode('utf-8')

    # _Message.setPayloadAsString
    #
    def setPayloadAsString(self, string):
        self.payloadAsString = string

    # _Message.getPayloadAsString
    #
    def getPayloadAsString(self):
        if self.payloadAsString:
            return self.payloadAsString

        log_debug("_Message.getPayloadAsString type(payload)",
                  type(self.payload))
        if isinstance(self.payload, dict):
            if 'payload' in self.payload:
                return json.dumps(self.payload['payload'])
            else:
                return json.dumps(self.payload)

        log_debug("_Message.getPayloadAsString type(payload)",
                  type(self.payload))
        return self.payload
