from ..target import Target
import hashlib
import dill
import joblib

class PyTarget(Target):
    def __init__(self, name, obj=None):
        self._name = name
        self._obj = obj
        super(PyTarget, self).__init__()
        if not obj is None:
            self.set(obj)

    def identifier(self):
        return self._name

    def get(self):
        return self._obj

    def set(self, obj):
        self._obj = obj

    def checksum(self):
        digest = super(PyTarget, self).checksum()
        if not self._obj is None:
            m = hashlib.sha1()
            m.update(digest.encode())
            m.update(joblib.hash(self._obj).encode())
            return m.hexdigest()
        else:
            return digest

    def is_damaged(self):

        if not self._obj is None:
            return False

        stored = self.stored()
        if stored and not stored._obj is None:
            self._obj = stored._obj
            return False

        return True


