import collections
from operator import getitem
import functools
import dask
import dask.threaded
import joblib
import os
import numpy

from .log import get_logger
logger = get_logger()

from .history import History
import task

history = History('.history.db')

class Target(object):
    def __init__(self):
        self.parent = task.get_current_task()
        self.force_update = False

    def exists(self):
        # Check if this target exists
        pass

    def get(self):
        # Get the underlying representation
        pass

    def timestamp(self):
        pass

    def __repr__(self):
        return self.__class__.__name__ + '(...)'

    def _get_props(self):
        # We take the index of this target
        # in its parent's output to derive a hash
        if not self.parent is None:
            outputs = self.parent.outputs()
            if isinstance(outputs, collections.Iterable):
                idx = outputs.index(self)
            else:
                # If it's the only output, we use a fixed string
                idx = "na"
            par = self.parent
        else:
            par = "na"
            idx = "na"
        return ((par, idx, repr(self)))

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return hash(self._get_props())

    def has_changed(self):
        global history

        if self.force_update:
            return True

        changed =  history.target_changed(self)

        return changed

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

class PyTarget(Target):
    def __init__(self, obj=None):
        self._obj = obj
        super(PyTarget, self).__init__()
        if not obj is None:
            self.set(obj)

    def _get_props(self):
        # We take the index of this target
        # in its parent's output to derive a hash
        if not self.parent is None:
            outputs = self.parent.outputs()
            if isinstance(outputs, collections.Iterable):
                idx = outputs.index(self)
            else:
                # If it's the only output, we use a fixed string
                idx = "na"
            par = self.parent
        else:
            par = "na"
            idx = "na"

        if isinstance(self._obj, numpy.ndarray):
            obj = str(self._obj.data)
        elif isinstance(self._obj, list):
            obj = str(self._obj)
        else:
            obj = self._obj
        return ((par, idx, obj, repr(self)))

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

def require(target, workers=1, update_from=None):
    global history

    if update_from:
        update_from.force_update = True
        logger.info('REQUIRE {} UPDATE FROM {}'.format(target, update_from))
    else:
        logger.info('REQUIRE {}'.format(target))

    # Create dask
    d = {}

    for t in task.Task.tasks:
        inputs = t.inputs()
        outputs = t.outputs()
        if not isinstance(outputs, collections.Iterable):
            outputs = (outputs,)

        for i, o in enumerate(outputs):
            d[o] = (id,) + inputs

        for inp in inputs:
            if isinstance(inp, Target) and not inp.parent:
                d[inp] = None

    tasklist = []
    targets = dask.core.toposort(d)
    for tar in targets:
        if tar.parent is None or tar.parent in tasklist:
            continue
        tasklist.append(tar.parent)

    for t in tasklist:
        outputs = t.outputs()
        if not isinstance(outputs, collections.Iterable):
            outputs = (outputs,)

        do_update = any(map(lambda i: isinstance(i, Target) and i.has_changed(), t.inputs()))
        do_update = do_update or any(map(lambda o: isinstance(o, Target) and not o.exists(), outputs))
        if do_update:
            for o in outputs:
                if isinstance(o, Target):
                    o.force_update = True
            # An input has changed: this task needs to be executed
            def runner(t, outputs, *args):
                for i in t.inputs():
                    if isinstance(i, Target):
                        assert i.exists()
                        history.add_target(i)
                t.run()
                for o in outputs:
                    if not o.exists():
                        raise RuntimeError('Task {} did not create target {}'.format(t, o))
                for o in outputs:
                    if isinstance(o, Target):
                        history.add_target(o)
        else:
            # We can skip this task
            def runner(t, outputs, *args):
                logger.info('SKIPPING {}'.format(t))
                pass

        # Bind current task to runner
        runner.__name__ = t.__class__.__name__
        runner = functools.partial(runner, t, outputs)


        for o in outputs:
            if isinstance(o, Target):
                d[o] = (runner,) + t.inputs()

    dask.threaded.get(dask.optimize.cull(d, target), target, nthreads=workers)

    logger.info('DONE {}'.format(target))

