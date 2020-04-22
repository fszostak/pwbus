# PWBus - EngineMonitorEvent Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Mon Dec 16 20:32:30 -03 2019

from datetime import datetime
from json import dumps
import traceback

from pwbus.commons.logging import *
from pwbus.connectors.redis import RedisConnector

# EngineMonitorEvent - Get engine monitor channel settings
#
#


class EngineMonitorEvent(RedisConnector):

    def __init__(self, connection=None):
        if not connection:
            super().__init__()
            with self.getConnectionPool().item() as conn:
                self.connection = conn
        else:
            self.connection = connection

    # EngineMonitorEvent.pushEvent
    #
    def pushEvent(self, value):
        try:
            value.update(
                {"datetime": datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
            self.connection.lpush('pwbus_monitor_events', dumps(value))
        except Exception as e:
            print('EngineMonitorEvent.send - Failed on push event', str(e))

    # EngineMonitorEvent.fetchEvent
    #
    def fetchEvent(self):
        try:
            event = self.connection.rpop('pwbus_monitor_events')
            if event:
                return ''.join(event.decode('utf-8'))
        except Exception as e:
            print('EngineMonitorEvent.send - Failed on push event', str(e))

    # EngineMonitorEvent.setData
    #
    async def setData(self, name, value):
        try:
            self.connection.set(name, dumps(value))
        except Exception as e:
            print('EngineMonitorEvent.setData - Failed on set data', str(e))

    # EngineMonitorEvent.getData
    #
    def getData(self, name):
        try:
            data = self.connection.get(name)
            if data:
                if isinstance(data, int):
                    return str(data)
                else:
                    return ''.join(data.decode('utf-8'))
        except Exception as e:
            print('EngineMonitorEvent.getData - Failed on get data', str(e))

    # EngineMonitorEvent.setValue
    #
    def setValue(self, name, value):
        try:
            self.connection.set(name, value)
        except Exception as e:
            print('EngineMonitorEvent.setValue - Failed on set value', str(e))

    # EngineMonitorEvent.incrValue
    #
    def incrValue(self, name):
        try:
            data = self.connection.incr(name, amount=1)
            if data:
                return str(data)
        except Exception as e:
            print('EngineMonitorEvent.incrValue - Failed on increment value', str(e))
            print(str(e))
