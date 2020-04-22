# PWBus - _TransformCSV Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sun Nov 17 14:07:58 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.transformations._transform import _Transform


class _TransformCSV(_Transform):

    # _TransformCSV.parsein - return Dict
    #
    def parse_in(self, payload):
        keys = []
        values = dict()

        for line in payload.splitlines():
            if not keys:

                for key in line.split(';'):
                    keys.append(key)
            else:
                i = 0
                for value in line.split(';'):
                    key = keys[i]
                    if key == '':
                        continue
                    values.update({key: value})
                    i += 1

        return values

    # _TransformCSV.parse_out - receive Dict
    #
    def parse_out(self, payload):
        payload_out = ''

        for key in payload:
            payload_out += f'{key};'
        payload_out += "\n"

        for key in payload:
            payload_out += f'{payload[key]};'
        payload_out += "\n"

        return payload_out
