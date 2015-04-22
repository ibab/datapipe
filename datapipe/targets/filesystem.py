import os
from ..target import Target

class LocalFile(Target):
    def __init__(self, path):
        self._path = path
        super(LocalFile, self).__init__()
        if self.exists():
            self._memory['timestamp'] = os.path.getmtime(self._path)
        else:
            self._memory['timestamp'] = 0

    def identifier(self):
        return self._path

    def exists(self):
        return os.path.exists(self._path)

    def path(self):
        return self._path

    def store(self, batch=None):
        if self.exists():
            self._memory['timestamp'] = os.path.getmtime(self._path)
        else:
            self._memory['timestamp'] = 0
        super(LocalFile, self).store(batch)

    def open(self, *args, **kwargs):
        return open(self._path, *args, **kwargs)

    def is_damaged(self):
        mem = self.stored()
        if mem is None or not 'timestamp' in mem:
            return True

        if not self.exists():
            return True

        return self._memory['timestamp'] > mem['timestamp']

