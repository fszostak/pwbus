# PWBus - DynamicTask Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Mon Nov 18 09:30:57 -03 2019

import importlib
import os.path
import traceback

from pwbus.commons.logging import *

# DynamicTask
#
#


class DynamicTask():

    def __init__(self, task_id, payload, isDebugEnabled=False, isProductionMode=True):
        try:
            task_name = task_id.split('.')
            module_name = f'pwbus_tasks.{task_name[0]}.{task_name[1]}'
            class_name = task_name[1].capitalize()
        except:
            log_debug(
                f'🟥 DynamicTask - Invalid task_id [{task_id}] specify "module.class" (lowercase)')
            payload['errorOnTask'] = f'Invalid task_id [{task_id}]'
            return

        try:
            module = importlib.import_module(module_name)
            if not isProductionMode:
               module = importlib.reload(module) 
            class_ = getattr(module, class_name)
            self.instance = class_(payload)

            if isDebugEnabled:
                log_debug(
                    f'DynamicTask - Module [{module_name}] Class [{class_name}] instance created')

        except:
            formatted_lines = traceback.format_exc().splitlines()
            log_debug(
                f'⚠️ DynamicTask - WARNING!!! Cannot instatiate class -  task_id [{task_id}] - Message: {formatted_lines[-2]} | {formatted_lines[-1]}')
            payload['errorOnTask'] = formatted_lines[-1]

            traceback.print_exc()
            raise

    # DynamicTask.getInstance
    #
    def getInstance(self):
        return self.instance

    # DynamicTask.isLoaded
    #
    def isLoaded(self):
        return True if self.instance else False
