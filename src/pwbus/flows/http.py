# PWBus - HttpFlowIn & HttpFlowOut Classes
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019

import requests
from requests.exceptions import HTTPError
import secrets
import traceback

from pwbus.commons.logging import *
from pwbus.flows._message import _Message
from pwbus.flows._flow import FlowIn, FlowOut
from pwbus.connectors.http import HttpConnectorIn, HttpConnectorOut

# HttpFlowIn
#
#


class HttpFlowIn(FlowIn):

    def __init__(self, request, host=None, port=None):
        super().__init__('in', HttpConnectorIn(host=host, port=port))
        self.request = request
        self.host = host
        self.port = port

    # HttpFlowIn.receive
    #
    def receive(self):

        if self.isDebugEnabled():
            log_debug(
                f'HttpFlowIn.receive - Started to [{self.getRecvFrom()}] format={self.getPayloadFormat()}')

        try:
            if self.request.content_type == 'application/json':
                payload_in = self.request.get_json()
            else:
                payload_in = self.request.get_data()

            for name, value in self.request.headers:
                if name in self.ALLOWED_HEADERS:
                    self.setHeadersEntry(name, value)

            if self.isDebugEnabled():
                log_debug(
                    f'↘️  HttpFlowIn.receive - Message received successfully from [{self.getRecvFrom()}]')
            return payload_in

        except:
            log_error(
                traceback, "HttpFlowIn.receive - Failed to receive message from '" + self.getRecvFrom() + "'")
            self.setResponse({"status_code": 500})
            return False

# HttpFlowOut
#
#


class HttpFlowOut(FlowOut):

    def __init__(self, host=None, port=None):
        super().__init__('out', HttpConnectorOut(host=host, port=port))
        self.host = host
        self.port = port

    # HttpFlowOut.send
    #
    def send(self):
        if self.isDebugEnabled():
            log_debug(f'HttpFlowOut.send - Started to [{self.getSendTo()}]')

        try:
            if self.getPayloadFormat() == 'json':
                response = requests.post(
                    self.getSendTo(),
                    json=self.getMessage().getPayload(),
                    headers=self.getHeaders()
                )
            else:
                response = requests.post(
                    self.getSendTo(),
                    data=self.getMessage().getPayloadStream(),
                    headers=self.getHeaders()
                )

            if response.status_code != 200:
                if self.isDebugEnabled():
                    log_debug(
                        f'HttpFlowOut.send - status_code={response.status_code}')
                return False

            if 'Content-Type'in response.headers:
                if response.headers['Content-Type'] == 'application/json':
                    self.setResponse(response.json())
                else:
                    self.setResponse(response.text)

            if self.isDebugEnabled():
                log_debug("↗️  HttpFlowOut.send - Message sent successfully - task_id=" +
                          self.getMessage().getTaskId())
            return True

        except:
            log_debug(
                f'HttpFlowOut.send - Failed to send the message to [{self.getSendTo()}]')

        self.setHeadersEntry("Pwbus-Status-Code", 500)

        return False
