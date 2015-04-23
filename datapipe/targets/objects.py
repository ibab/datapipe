from ..target import Target
import hashlib
import dill
import joblib
import binascii

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
        self._memory['obj'] = binascii.hexlify(dill.dumps(obj))

    def is_damaged(self):
        mem = self.stored()
        if mem and 'obj' in mem:
            if self._obj is None:
                self._memory['obj'] = mem['obj']
                self._obj = dill.loads(mem['obj'].decode('base64'))
                return self._obj is None
            else:
                return joblib.hash(self._obj) != joblib.hash(dill.loads(binascii.unhexlify(mem['obj'])))
        else:
            return self._obj is None

