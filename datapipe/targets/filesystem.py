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
        if mem is None or 'timestamp' not in mem:
            return True

        if not self.exists():
            return True

        return self._memory['timestamp'] > mem['timestamp']

class LocalDirectory(Target):
    def __init__(self, path):
        self._path = path
        super(LocalDirectory, self).__init__()

        self._memory['timestamps'] = self.get_mtimes()

    def exists(self):
        return os.path.isdir(self._path)

    def get_mtimes(self):
        mtimes = dict()
        if self.exists():
            for root, dirs, files in os.walk(self._path):
                for f in files:
                    f = os.path.join(root, f)
                    mtimes[f] = os.path.getmtime(f)
        return mtimes

    def store(self, batch=None):
        self._memory['timestamps'] = self.get_mtimes()
        super(LocalDirectory, self).store(batch)

    def path(self):
        return self._path

    def identifier(self):
        return self.path()

    def paths(self):
        for root, dirs, files in os.walk('self._path'):
            for f in files:
                yield f

    def is_damaged(self):
        mem = self.stored()
        if mem is None or 'timestamps' not in mem:
            return True

        if not self.exists():
            return True

        for name, ts in mem['timestamps'].items():
            if name not in self._memory['timestamps'] or self._memory['timestamps'][name] > ts:
                return True

        return False
