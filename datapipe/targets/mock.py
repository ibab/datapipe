
from ..target import Target


class MockTarget(Target):
    def __init__(self, identifier):
        self._identifier = identifier
        super(self.__class__, self).__init__()

    def identifier(self):
        return self._identifier

    def is_damaged(self):
        return True
