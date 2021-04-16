# PWBus - Logging library
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 08:49:20 -03 2019

from datetime import datetime
from time import time
from threading import current_thread
from os import environ

from pwbus.engines.engine_monitor_event import EngineMonitorEvent
from pwbus.commons.cprint import cprint, cprint_rainbow
from sys import stdout, exit
import random

LOG_DEBUG = True
LOG_ERROR = True
date_time = None
debug_color = 'green'

# logging.set_debug
#
def set_debug(enabled):
    global LOG_DEBUG
    LOG_DEBUG = enabled

# logging.set_debug_color
#
def set_debug_color(color):
    global debug_color
    debug_color = color


# logging.print_datetime
#
def print_datetime(arg1, arg2=''):
    global date_time
    global debug_color
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    this_thread = current_thread().getName()
    this_thread = "{:<10}".format(this_thread)

    if not date_time or now[0:9] != date_time[0:9]:
        date_time = now
        print(this_thread, now, '|-------------------------------------------------------------------')

    try:
        print(f'{this_thread[0:10]} {now[10:]} ', end='')
        if environ['ENVIRONMENT'] == 'production':
            print(f'{arg1} {arg2}')
        else:
            cprint(arg1, color='bright_white')
            cprint_rainbow(f' {arg2}', key=this_thread[0:10])
            print()
    except:
        cprint(f'{this_thread[0:10]} {now[10:]} ', color='bright_black')
        cprint(arg1, color='bright_white')
        cprint(f' {arg2}', color=debug_color)
        print()

# logging.getMillis
#


def getMillis():
    return int(round(time() * 1000))


# logging.log_debug
#
def log_debug_success(arg1, arg2=''):
    global LOG_DEBUG
    if LOG_DEBUG:
        print_datetime('[GREAT] 👍', arg1)
        if arg2 != '':
            print_datetime('       👍', arg2)
        print("\n")
        stdout.flush()


# logging.log_debug
#
def log_debug(arg1, arg2=''):
    global LOG_DEBUG
    if LOG_DEBUG:
        print_datetime('[DEBUG]::', arg1)
        if arg2 != '':
            print_datetime('       ::', arg2)
        stdout.flush()


# logging.log_debug_var
#
def log_debug_var(key, value, arg1=''):
    global LOG_DEBUG
    if LOG_DEBUG:
        print_datetime(f'[DEBUG]>> {key} = [{value}]')
        if arg1 != '':
            print_datetime('       >>', arg1)
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
            f'[ERROR]## 🟥 {message}\n       ## Message: {formatted_lines[-1]}')
        traceback.print_exc()
        stdout.flush()

# logging.log_fatal
#


def log_fatal(message):
    EngineMonitorEvent().pushEvent(
        {"type": "fatal", "message": message, "exception": ""})
    print_datetime('[FATAL]!! 🟥 Exiting with fatal error -', message)
    exit()


# logging.log_message_dump
#
def log_message_dump(message, message_dump):
    global LOG_DEBUG
    if LOG_DEBUG:
        if message_dump:
            print_datetime(f'[DUMP] .. {message} - message: {message_dump}')
        else:
            print_datetime(f'[DUMP] .. {message} - empty message')
        stdout.flush()

# logging.log_exit
#


def log_exit(arg1):
    print_datetime('[EXIT] !! 🟥 ', arg1)
    exit()

# logging.log_warn
#


def log_warn(arg1, arg2=''):
    global LOG_DEBUG
    if LOG_DEBUG:
        print_datetime('[WARN]  :: ⚠️ ', arg1)
        if arg2 != '':
            print_datetime('       ::', arg2)
        stdout.flush()
