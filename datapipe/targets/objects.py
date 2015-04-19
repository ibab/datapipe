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

    def is_damaged(self):

        stored = self.stored()
        if stored:
            if self._obj is None:
                self._obj = stored._obj
                return stored._obj is None
            else:
                return joblib.hash(self._obj) == joblib.hash(stored._obj)
        else:
            return self._obj is None

