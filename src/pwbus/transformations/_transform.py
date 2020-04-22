# PWBus - _Transform Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sun Nov 17 14:07:58 -03 2019

import json
import traceback

from pwbus.commons.logging import *


class _Transform():

    # _Transform.parse_in - receive <any> return <dict>
    #
    def parse_in(self, payload):
        pass

    # _Transform.parse_out - receive <dict> return <any>
    #
    def parse_out(self, payload):
        pass

    # _Transform.jsonToString
    #
    def stringToJson(self, data):
        try:
            if isinstance(data, str):
                return json.loads(data)
            if isinstance(data, dict):
                return data
            else:
                log_debug(
                    f'_Transform.stringToJson - Invalid type [{type(data)}]', data)
                return None

        except json.decoder.JSONDecodeError:
            log_debug("_Transform.jsonToString - Invalid JSON")
            log_message_dump('_Transform.jsonToString', payload_in)

    # _Transform.jsonToString
    #
    def jsonToString(self, data):
        if isinstance(data, dict):
            return json.dumps(data)
        if isinstance(data, str):
            return data
        else:
            log_debug(
                f'_Transform.jsonToString - Invalid type [{type(data)}]', data)
            return None
