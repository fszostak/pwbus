# PWBus - Logging library
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 08:49:20 -03 2019

from datetime import datetime
from time import time
from threading import current_thread

from pwbus.engines.engine_monitor_event import EngineMonitorEvent

from sys import stdout, exit

LOG_DEBUG = True
LOG_ERROR = True

# logging.print_datetime
#


def print_datetime(arg1, arg2=''):
    print(current_thread().getName(), datetime.now().strftime(
        '%Y-%m-%d %H:%M:%S.%f'), f'{arg1} {arg2}')

# logging.getMillis
#


def getMillis():
    return int(round(time() * 1000))

# logging.log_debug
#


def log_debug_success(arg1, arg2=''):
    global LOG_DEBUG
    if LOG_DEBUG:
        print_datetime('SUCES$$ 👍 ', arg1)
        if arg2 != '':
            print_datetime('     $$', arg2)
        print("\n")
        stdout.flush()

# logging.log_debug
#


def log_debug(arg1, arg2=''):
    global LOG_DEBUG
    if LOG_DEBUG:
        print_datetime('Debug::', arg1)
        if arg2 != '':
            print_datetime('     ::', arg2)
        stdout.flush()

# logging.log_debug_var
#


def log_debug_var(key, value, arg1=''):
    global LOG_DEBUG
    if LOG_DEBUG:
        print_datetime(f'Debug>> {key} = [{value}]')
        if arg1 != '':
            print_datetime('     >>', arg1)
        stdout.flush()

# logging.log_error
#


def log_error(traceback, message):
    global LOG_ERROR
    EngineMonitorEvent().pushEvent(
        {"type": "error", "message": message, "exception": traceback.format_exc()})
    if LOG_DEBUG:
        formatted_lines = traceback.format_exc().splitlines()
        print_datetime(
            f'ERROR## 🟥 {message}\n     ## Message: {formatted_lines[-1]}')
        traceback.print_exc()
        stdout.flush()

# logging.log_fatal
#


def log_fatal(message):
    EngineMonitorEvent().pushEvent(
        {"type": "fatal", "message": message, "exception": ""})
    print_datetime('FATAL!! 🟥 Exiting with fatal error -', message)
    exit()


# logging.log_message_dump
#
def log_message_dump(message, message_dump):
    global LOG_DEBUG
    if LOG_DEBUG:
        if message_dump:
            print_datetime(f'DUMP .. {message} - message: {message_dump}')
        else:
            print_datetime(f'DUMP .. {message} - empty message')
        stdout.flush()

# logging.log_exit
#


def log_exit(arg1):
    print_datetime('EXIT !! 🟥 ', arg1)
    exit()

# logging.log_warn
#


def log_warn(arg1, arg2=''):
    global LOG_DEBUG
    if LOG_DEBUG:
        print_datetime('WARN:: ⚠️ ', arg1)
        if arg2 != '':
            print_datetime('     ::', arg2)
        stdout.flush()

