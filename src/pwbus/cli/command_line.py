#!/usr/bin/env python3

# PWBus - Main
#:
#:  maintainer: fabio.szostak@gmail.com | Tue Nov 19 11:17:54 -03 2019

__author__ = "fszostak@gmail.com"
__version__ = "0.1.64"

import sys
import os.path
import getopt

from pwbus.engines.engine_manager import EngineManager


def main():
    argv = sys.argv[1:]

    channel = None
    engine = None
    command = None
    registry_file = None

    help_message = f'\npwbus -f <registry-file> [-v] [-e <engine>] [-c <channel>] start\n\nPWBUS Core - Version {__version__}\n\n      -v  version\n      -f  <registry-file>\n      --registry=<registry-config-file>\n      -e <redis|socket>\n      ----engine=<redis|socket>\n\n      -c <channel_id>\n      --channel=channel_id\n\n      start the pwbus server\n'

    try:
        opts, args = getopt.getopt(argv, "hve:c:f:", ["start|registry=|channel=|engine="])
    except getopt.GetoptError:
        print(help_message)
        sys.exit(2)

    for arg in args:
        if arg == "start":
            command = arg

    for opt, arg in opts:
        if opt == '-h':
            print(help_message)
            sys.exit()
        elif opt == '-v':
            print(__version__)
            sys.exit()
        elif opt in ("-e", "--engine"):
            engine = arg
        elif opt in ("-c", "--channel"):
            channel = arg
        elif opt in ("-f", "--registry-file"):
            registry_file = arg

    try:
        if not registry_file:
            if os.path.isfile("./etc/pwbus_registry.json"):
                registry_file = "./etc/pwbus_registry.json"
            else:
                print("\nError: Registry file is not defined")
        else:
            if command:
                engine_manager = EngineManager(registry_file)
                engine_manager.action(command, engine, channel)
                sys.exit(0)

    except:
        sys.exit(1)

    print(help_message)


# __main__
#
if __name__ == "__main__":
    main()
