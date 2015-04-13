import collections
from operator import getitem
import functools
import dask
import dask.threaded
import os

from .log import get_logger
logger = get_logger()

from .history import History
from .task import Task, get_current_task

class Target(object):
    def __init__(self):
        self.parent = get_current_task()
        self.force_update = False

    def exists(self):
        # Check if this target exists
        pass

    def get(self):
        # Get the underlying representation
        pass

    def timestamp_contents(self):
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

    def timestamp_contents(self):
        return os.path.getmtime(self.path)

    def exists(self):
        return os.path.exists(self.path)

    def get(self):
        return self.path

def is_uptodate(target):
    # TODO make this more efficient by calculating the status for all targets once (in require?)

    if target.force_update:
        return False

    if not target.exists():
        return False

    if target.parent:
        inputs = filter(lambda o: isinstance(o, Target), target.parent.inputs())
        upstream_uptodate = all(map(is_uptodate, inputs))
        upstream_timestamps = list(map(lambda o: o.timestamp_contents(), filter(lambda o: o.exists(), inputs)))
        if not upstream_uptodate:
            return False
        elif upstream_timestamps and target.timestamp_contents() < max(upstream_timestamps):
            return False

    return True

def require(target, workers=1, update_from=None):


    if update_from:
        update_from.force_update = True
        logger.info('REQUIRE {} UPDATE FROM {}'.format(target, update_from))
    else:
        logger.info('REQUIRE {}'.format(target))

    d = {}

    for t in Task.tasks:
        inputs = t.inputs()
        outputs = t.outputs()

        if not isinstance(outputs, collections.Iterable):
            outputs = (outputs,)

        if all(map(lambda o: is_uptodate(o), filter(lambda o: isinstance(o, Target), outputs))):
            # We can skip this task
            def runner(t, *args):
                logger.info('SKIPPING {}'.format(t))
        else:
            # This task needs to be executed
            def runner(t, *args):
                t.run()

        # Bind current task to runner
        runner.__name__ = t.__class__.__name__
        runner = functools.partial(runner, t)

        for i, o in enumerate(outputs):
            d[o] = (runner,) + inputs

        for inp in inputs:
            if isinstance(inp, Target) and not inp.parent:
                d[inp] = None
    
    dask.threaded.get(d, target, nthreads=workers)

    logger.info('DONE {}'.format(target))

