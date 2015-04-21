import os
from ..target import Target

class LocalFile(Target):
    def __init__(self, path):
        self._path = path
        super(LocalFile, self).__init__()
        self._timestamp = 0

    def identifier(self):
        return self._path

    def exists(self):
        return os.path.exists(self._path)

    def path(self):
        return self._path

    def open(self, *args, **kwargs):
        return open(self._path, *args, **kwargs)

    def store(self, batch=None):
        if self.exists():
            self._memory['timestamp'] = os.path.getmtime(self._path)
        else:
            self._memory['timestamp'] = 0
        super(LocalFile, self).store(batch)

    def is_damaged(self):
        stored = self.stored()
        if stored is None:
            return True
        if self.exists():
            return os.path.getmtime(self._path) > stored['timestamp']
        else:
            return True

