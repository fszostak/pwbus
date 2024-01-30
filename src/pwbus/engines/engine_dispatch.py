# PWBus - EngineDispatch Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sun Nov 17 07:25:07 -03 2019

from time import sleep
import importlib
import traceback

from pwbus.commons.logging import *
from pwbus.transformations.transformation import Transformation
from pwbus.tasks.dynamic_task import DynamicTask
from pwbus.engines.engine_monitor_event import EngineMonitorEvent


# EngineDispatch
#
#
class EngineDispatch():

    # RedisEngine.serve
    #
    def route(self, channel_registry, request=None):

        try:
            if not channel_registry or not channel_registry['channel']:
                log_debug('EngineDispatch.route - registry is invalid')
                return None

            DEBUG = channel_registry['engine.debug']
            try:
                PRODUCTION = channel_registry['engine.production']
            except:
                PRODUCTION = True

            if DEBUG:
                log_debug(
                    f'EngineDispatch.route - Message dispatch started for [{channel_registry["channel"]}] in {"production" if PRODUCTION else "development"} mode')

            # @@@ step 1 - retrieve message in

            # Flow input

            # Get FlowIn Class dynamically
            resource_type = channel_registry["flow.in.resource_type"].lower()
            module_name = f'pwbus.flows.{resource_type}'
            class_name = f'{resource_type.capitalize()}FlowIn'
            module = importlib.import_module(module_name)
            class_ = getattr(module, class_name)
            flow_in = class_(
                request=request,
                host=channel_registry["flow.in.host"] if "flow.in.host" in channel_registry else None,
                port=channel_registry["flow.in.port"] if "flow.in.port" in channel_registry else None
            )

            flow_in.setRegistry(channel_registry)

            # receive payload
            payload_in = flow_in.receive()
            if payload_in is None:
                flow_in.getMessage().setHeadersEntry("Pwbus-Status-Code", 500)
                flow_in.getMessage().setHeadersEntry("Pwbus-Info-Message",
                                                     "Failed on receive request message")
                EngineMonitorEvent().incrValue("pwbus_flow_errors")
                EngineMonitorEvent().incrValue("pwbus_transformation_errors")
                sleep(2)
                return None

            # start racing
            start_racing = getMillis()
            if DEBUG:
                log_debug(f'▶️  EngineDispatch.route - Start message flow')

            # transform payload in
            transformation = Transformation()
            transformed_payload = transformation.execute_in(
                channel_registry,
                payload_in
            )

            # prepare message in
            flow_in.prepareMessage(transformed_payload)

            # @@@ step 2 - execute task
            if flow_in.isTaskEnabled():
                field_task_id = flow_in.getMessage().getTaskId()
                if field_task_id is not None:
                    if DEBUG:
                        log_debug(
                            f'EngineDispatch - Flow running for task_id [{field_task_id}]')

                    try:
                        for task_id in field_task_id.split(','):
                            task = DynamicTask(
                                task_id, transformed_payload, DEBUG, PRODUCTION)
                            task.getInstance().execute()
                    except:
                        EngineMonitorEvent().incrValue("pwbus_task_errors")
                        pass

            # @@@ step 3 - send message out

            # Get FlowOut Class dynamically
            resource_type = channel_registry["flow.out.resource_type"].lower()
            module_name = f'pwbus.flows.{resource_type}'
            class_name = f'{resource_type.capitalize()}FlowOut'
            module = importlib.import_module(module_name)
            class_ = getattr(module, class_name)
            flow_out = class_(
                host=channel_registry["flow.out.host"] if "flow.out.host" in channel_registry else None,
                port=channel_registry["flow.out.port"] if "flow.out.port" in channel_registry else None
            )

            # prepare message out
            flow_out.setResponse(flow_in.getResponse())
            flow_out.setHeaders(flow_in.getHeaders())
            message = flow_in.getMessage()

            if transformed_payload is None:
                if DEBUG:
                    log_debug(
                        f'🟥 EngineDispatch.route - Failed on transform response message')
                EngineMonitorEvent().incrValue("pwbus_flow_errors")
                return None

            if DEBUG:
                log_debug(
                    f'EngineDispatch.route - transformed_payload=[{transformed_payload}]')

            # Move headers entries to payload
            headers = flow_in.getHeaders()
            for field in headers:
                transformed_payload[field] = headers[field]

            message.setPayload(transformed_payload)

            # transform payload out
            transformation = Transformation()
            transformed_payload = transformation.execute_out(
                channel_registry,
                message.getPayload()
            )

            message.setPayloadAsString(transformed_payload)

            # send message out
            flow_out.setRegistry(channel_registry)
            flow_out.prepareMessage(message, headers)

            if channel_registry["flow.message_dump"]:
                log_message_dump('EngineDispatch.route', transformed_payload)

            # end message transformation
            end_transform = getMillis()

            if flow_out:
                if not flow_out.send():
                    if DEBUG:
                        log_debug(
                            f'🟥 EngineDispatch.route ({channel_registry["flow.out.resource_type"]}) - Failed on send response message - Waiting 10s...')
                    EngineMonitorEvent().incrValue("pwbus_flow_errors")
                    sleep(2)
                    return None

            # end racing
            end_racing = getMillis()

            if DEBUG:
                log_debug(
                    f'✅ EngineDispatch.route - End message flow - Completed in {end_racing-start_racing}ms - delivery {(end_racing-start_racing)-(end_transform-start_racing)}ms - correlation_id [{flow_in.getMessage().getCorrelationId()}]\n\n')

            EngineMonitorEvent().incrValue("pwbus_flow_success")
            return flow_out

        except InterruptedError:
            log_debug(f'EngineDispatch.route - Stopping flow')
            return None

        except:
            traceback.print_exc()
            log_error(
                traceback, f'EngineDispatch.route - Error in flow execution')
