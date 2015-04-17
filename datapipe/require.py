import dask
import collections

from .history import History
from .task import Task
from .target import Target
from .log import get_logger

logger = get_logger()

def require(targets, workers=1, update_from=None):
    history = History('.history.db')

    if isinstance(targets, collections.Iterable):
        targets = list(targets)
    else:
        targets = [targets]

    if update_from:
        update_from.force_update = True
        logger.info('REQUIRE {} UPDATE FROM {}'.format(', '.join(map(str, targets)), update_from))
    else:
        logger.info('REQUIRE {}'.format(', '.join(map(str, targets))))

    # Create dask
    d = {}

    for t in Task.tasks:
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
    trgts = dask.core.toposort(d)
    for tar in trgts:
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

    dask.threaded.get(dask.optimize.cull(d, targets), targets, nthreads=workers)

    logger.info('DONE {}'.format(', '.join(map(str, targets))))

