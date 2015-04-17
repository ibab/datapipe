from ..target import Target

class PyTarget(Target):
    def __init__(self, obj=None):
        self._obj = obj
        super(PyTarget, self).__init__()
        if not obj is None:
            self.set(obj)

    def _get_entry(self):
        global history
        return history.get_target(self)

    def _set_entry(self):
        global history
        return history.add_target(self)

    def exists(self):
        ret = self._get_entry()
        return ret and os.path.exists(ret[3])

    def get(self):
        idx, hsh, ts, path = self._get_entry()
        return joblib.load(path)

    def set(self, obj):
        global history
        self._set_entry()
        idx, hsh, ts, path = self._get_entry()
        joblib.dump(obj, path)

    def __repr__(self):
        return  self.__class__.__name__ + '(object)'

