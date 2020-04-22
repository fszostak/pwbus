# PWBus - EngineMonitor Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Mon Dec 16 13:05:27 -03 2019

from threading import Thread
import asyncio
from datetime import datetime
import websockets
from json import dumps
import traceback

from pwbus.commons.logging import *
from pwbus.connectors.redis import RedisConnector
from pwbus.engines.engine_monitor_event import EngineMonitorEvent

# EngineMonitor - Get engine monitor channel settings
#
#


class EngineMonitor(Thread, RedisConnector):
    def __init__(self):
        Thread.__init__(self)
        RedisConnector.__init__(self)

    async def monitor(self, websocket, path):
        try:
            with self.getConnectionPool().item() as connection:
                event = EngineMonitorEvent(connection)
                while True:
                    data = {
                        "pwbus_task_unique_id": ''.join(connection.get("pwbus_unique").decode('utf-8')),
                        "pwbus_event": event.fetchEvent(),
                        "pwbus_flow_success": event.getData("pwbus_flow_success"),
                        "pwbus_flow_errors": event.getData("pwbus_flow_errors"),
                        "pwbus_transformation_errors": event.getData("pwbus_transformation_errors"),
                        "pwbus_task_errors": event.getData("pwbus_task_errors")
                    }
                    await websocket.send(dumps(data))
                    await asyncio.sleep(2)
        except:
            pass

    # EngineMonitor.run
    #
    def run(self):
        try:
            with self.getConnectionPool().item() as connection:
                event = EngineMonitorEvent(connection)
                event.pushEvent(
                    {"type": "info", "message": "PWBus Started", "exception": ""})
                event.setValue("pwbus_flow_success", 0)
                event.setValue("pwbus_flow_errors", 0)
                event.setValue("pwbus_transformation_errors", 0)
                event.setValue("pwbus_task_errors", 0)

            # Start server monitor
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            start_server = websockets.serve(self.monitor, "0.0.0.0", 8888)
            asyncio.get_event_loop().run_until_complete(start_server)
            log_debug('👁️  EngineMonitor.run - Server monitor started')
            asyncio.get_event_loop().run_forever()

        except:
            pass
