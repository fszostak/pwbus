# PWBus - cprint - color print
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Tue Apr 28 18:17:12 -03 2020

COLORS = True

bcolors = {
    "black":          u'\u001b[30m',
    "red":            u'\u001b[31m',
    "green":          u'\u001b[32m',
    "yellow":         u'\u001b[33m',
    "blue":           u'\u001b[34m',
    "magenta":        u'\u001b[35m',
    "cyan":           u'\u001b[36m',
    "white":          u'\u001b[37m',
    "bright_black":   u'\u001b[40m\u001b[1m',
    "bright_red":     u'\u001b[41m\u001b[1m',
    "bright_green":   u'\u001b[42m\u001b[1m',
    "bright_yellow":  u'\u001b[43m\u001b[1m',
    "bright_blue":    u'\u001b[44m\u001b[1m',
    "bright_magenta": u'\u001b[45m\u001b[1m',
    "bright_cyan":    u'\u001b[46m\u001b[1m',
    "bright_white":   u'\u001b[47m\u001b[1m',
}

fcolors = {
    "reset":          u'\u001b[0m',
    "bold":           u'\u001b[1m',
    "black":          u'\u001b[30m',
    "red":            u'\u001b[31m',
    "green":          u'\u001b[32m',
    "yellow":         u'\u001b[33m',
    "blue":           u'\u001b[34m',
    "magenta":        u'\u001b[35m',
    "cyan":           u'\u001b[36m',
    "white":          u'\u001b[37m',
    "bright_black":   u'\u001b[30m\u001b[1m',
    "bright_red":     u'\u001b[31m\u001b[1m',
    "bright_green":   u'\u001b[32m\u001b[1m',
    "bright_yellow":  u'\u001b[33m\u001b[1m',
    "bright_blue":    u'\u001b[34m\u001b[1m',
    "bright_magenta": u'\u001b[35m\u001b[1m',
    "bright_cyan":    u'\u001b[36m\u001b[1m',
    "bright_white":   u'\u001b[37m\u001b[1m',
}


def cprint(data, color='yellow', bcolor='black', enable=COLORS):
    print(f"{bcolors[bcolor]}{fcolors[color]}{data}{fcolors['reset']}" if enable else data, end='')
