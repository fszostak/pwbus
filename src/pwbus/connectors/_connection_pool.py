### PWBus - _ConnectionPool Class
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Fri Dec 13 19:17:38 -03 2019

### _ConnectionPoolState
#
class _ConnectionPoolState:
    _shared_state = {}
    def __init__(self):
        self.__dict__ = self._shared_state

### _ConnectionPool (Singleton)
#
class _ConnectionPool( _ConnectionPoolState ):
    def __init__(self):
        _ConnectionPoolState.__init__(self)
        self.state = dict()

    def __str__(self): return self.state

    def get(self, key):
        return self.state[key] \
               if key in self.state else None

    def update(self, connector_type, connection_pool):
        self.state.update({connector_type: connection_pool})

    def exists(self, connector_type):
        return True if connector_type in self.state else False
