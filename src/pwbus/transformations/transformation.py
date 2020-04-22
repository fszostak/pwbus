# PWBus - Transformation Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sun Nov 17 14:07:58 -03 2019

import traceback

from pwbus.commons.logging import *
from pwbus.transformations._transform_json import _TransformJSON
from pwbus.transformations._transform_csv import _TransformCSV


class Transformation():

    # Transformation.execute_in
    #
    def execute_in(self, registry, data):

        # TODO Dynamic Class
        if registry["flow.in.payload_format"] == 'csv':
            transform = _TransformCSV()
        elif registry["flow.in.payload_format"] == 'json':
            transform = _TransformJSON()
        else:
            log_debug(
                f'Transformation.execute - no transformation found for [{registry["flow.in.payload_format"]}]')
            return data

        if transform:
            return transform.parse_in(data)

        if registry['engine.debug']:
            log_debug(f'Transformation.execute - no transformation')

        return data

    # Transformation.execute_out
    #
    def execute_out(self, registry, data):

        # TODO Dynamic Class
        if registry["flow.out.payload_format"] == 'csv':
            transform = _TransformCSV()
        elif registry["flow.out.payload_format"] == 'json':
            transform = _TransformJSON()
        else:
            log_debug(
                f'Transformation.execute - no transformation found for [{registry["flow.out.payload_format"]}]')
            return data

        if transform:
            return transform.parse_out(data)

        return data
