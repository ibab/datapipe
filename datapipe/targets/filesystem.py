import os
from ..target import Target

class LocalFile(Target):
    def __init__(self, path):
        super(LocalFile, self).__init__()
        self._path = path
        self._handle = None

    def timestamp(self):
        return os.path.getmtime(self._path)

    def exists(self):
        return os.path.exists(self._path)

    def path(self):
        return self._path

    def get(self):
        return self.path()

    def open(self, *args, **kwargs):
        self._handle = open(self._path, *args, **kwargs)
        return self._handle

    def close(self):
        self._handle.close()

    def __repr__(self):
        return self.__class__.__name__ + '(' + repr(self.path()) + ')'

    def has_changed(self, other):
        return super(self.__class__).has_changed(other) or self.timestamp() > other.timestamp()

