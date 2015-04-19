import os
from ..target import Target

class LocalFile(Target):
    def __init__(self, path):
        self._path = path
        super(LocalFile, self).__init__()
        self._handle = None
        self._timestamp = 0

    def identifier(self):
        return self._path

    def exists(self):
        return os.path.exists(self._path)

    def path(self):
        return self._path

    def open(self, *args, **kwargs):
        self._handle = open(self._path, *args, **kwargs)
        return self._handle

    def close(self):
        self._handle.close()

    def store(self):
        self._handle = None
        if self.exists():
            self._timestamp = os.path.getmtime(self._path)
        super(LocalFile, self).store()

    def is_damaged(self):
        stored = self.stored()
        if stored is None:
            return True
        if self.exists():
            return os.path.getmtime(self._path) > stored._timestamp
        else:
            return True

