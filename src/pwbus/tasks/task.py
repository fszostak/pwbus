# PWBus - Task Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Mon Nov 18 10:58:09 -03 2019

import importlib
from time import sleep
from datetime import datetime
import traceback

from pwbus.commons.logging import *
from pwbus.tasks.task_unique_id import TaskUniqueId

# Task
#
#


class Task():

    def __init__(self, payload):
        self.payload = payload
        self.setField('__pwbus-task-unique-id__', TaskUniqueId().get())

    # Task.execute
    #
    def execute(self):
        self.main()
        return self.payload

    # Task.main - override this in your task
    #
    def main(self):
        return None

    # Task.setField
    #
    def setField(self, name, value):
        self.payload[name] = value

    # Task.getField
    #
    def getField(self, name):
        return self.payload[name]

    # Task.deleteField
    #
    def deleteField(self, name):
        del self.payload[name]

    # Task.getDateTime
    #
    def getDateTime(self):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    # Task.getDateTimeFormat
    #
    def getDateTimeFormat(self, my_format):
        return datetime.now().strftime(my_format)

    # Task.sleep
    #
    def sleep(self, ms):
        sleep(ms)
