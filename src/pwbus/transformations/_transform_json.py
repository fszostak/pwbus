# PWBus - _TransformJSON Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Mon Nov 18 07:36:30 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.transformations._transform import _Transform


class _TransformJSON(_Transform):

    # _TransformJSON.parsein - return Dict
    #
    def parse_in(self, payload):
        return self.stringToJson(data=payload)

    # _TransformJSON.parse_out - receive Dict
    #
    def parse_out(self, payload):
        return self.jsonToString(data=payload)

