# PWBus - Client Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Dec 14 19:27:34 -03 2019

# Client
#
#


class Client:

    # Client.clear_header
    #
    def clear_header(self, response):
        for field in list(response):
            if field.lower().startswith('pwbus-'):
                del response[field]
        return response
