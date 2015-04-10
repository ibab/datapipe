import collections
from operator import getitem
import functools
import dask
import dask.threaded

from .task import Task, get_current_task

class Target(object):
    def __init__(self):
        self.parent = get_current_task()

    def newer(self, targets):
        # Check if this target is newer than the specified targets
        pass

    def exists(self):
        # Check if this target exists
        pass

    def get(self):
        # Get the underlying representation
        pass

    def __repr__(self):
        return self.__class__.__name__ + '(' + repr(self.get()) + ')'

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == repr(other)

class LocalFile(Target):
    def __init__(self, path):
        super(LocalFile, self).__init__()
        self.path = path

    def clone(self, path=None):
        if path is None:
            return LocalFile(self.path)
        else:
            return LocalFile(path)

    def exists(self):
        import os
        return os.path.exists(self.path)

    def get(self):
        return self.path

    def from_suffix(self, suf, app):
        return self.clone(path=self.path.replace(suf, app))

def require(target, workers=1):
    d = {}

    for t in Task.tasks:
        inputs = t.inputs()
        outputs = t.outputs()

        def runner(t, *args):
            t.run()
        # Bind current task to runner
        runner.__name__ = t.__class__.__name__
        runner = functools.partial(runner, t)

        if not isinstance(outputs, collections.Iterable):
            outputs = (outputs,)

        for i, o in enumerate(outputs):
            d[o] = (runner,) + inputs

        for inp in inputs:
            if isinstance(inp, Target) and not inp.parent:
                d[inp] = None
    
    dask.threaded.get(d, target, nthreads=workers)

