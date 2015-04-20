from . import dask
import collections
import functools

from .task import Task
from .target import Target
from .log import get_logger
from .util import full_traverse

logger = get_logger()

def require(targets, workers=1):

    if isinstance(targets, collections.Iterable):
        targets = list(targets)
    else:
        targets = [targets]

    needs_update = set()

    for trg in Target.targets:
        if trg.is_damaged():
            needs_update.add(trg)

    # Create dask
    d = {}

    for t in Task.tasks:
        inputs = list(t.inputs())
        outputs = t.outputs()

        if not isinstance(outputs, collections.Iterable):
            outputs = [outputs,]
        else:
            outputs = list(outputs)

        for i, inp in enumerate(inputs):
            if isinstance(inp, list):
                inputs[i] = tuple(inp)
        for i, outp in enumerate(outputs):
            if isinstance(outp, list):
                inputs[i] = tuple(outp)

        for i, o in enumerate(outputs):
            d[o] = (id,) + tuple(full_traverse(inputs))

        for inp in full_traverse(inputs):
            if isinstance(inp, Target) and not inp.parent:
                d[inp] = None

    known_tasks = set()
    tasklist = []
    trgts = dask.toposort(d)
    for tar in trgts:
        if (not tar.parent) or (tar.parent in known_tasks):
            continue
        tasklist.append(tar.parent)
        known_tasks.add(tar.parent)

    for t in tasklist:
        outputs = t.outputs()
        inputs = t.inputs()
        if not isinstance(outputs, collections.Iterable):
            outputs = (outputs,)

        outputs_need_update = any(map(lambda o: isinstance(o, Target) and o in needs_update, full_traverse(outputs)))
        inputs_need_update = any(map(lambda i: isinstance(i, Target) and i in needs_update, full_traverse(inputs)))

        if outputs_need_update or inputs_need_update:
            for o in outputs:
                if isinstance(o, Target):
                    needs_update.add(o)
            # An input has changed: this task needs to be executed
            def runner(t, outputs, *args):
                t.run()
                for trg in full_traverse(t.outputs()):
                    trg.store()
        else:
            # We can skip this task
            def runner(t, outputs, *args):
                pass

        # Bind current task to runner
        runner.__name__ = t.__class__.__name__
        runner = functools.partial(runner, t, outputs)

        for o in outputs:
            if isinstance(o, Target):
                d[o] = (runner,) + tuple(full_traverse(t.inputs()))

    dask.get(dask.cull(d, targets), targets, nthreads=workers)

    Target.clear_store()
    for trg in Target.targets:
        trg.store()

    logger.info('DONE {}'.format(', '.join(map(str, targets))))

