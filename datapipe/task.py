import six
import types
import collections
import inspect
import numpy as np

from .log import get_logger
from .input import Input
from . import target

logger = get_logger()

def full_traverse(obj):
    if isinstance(obj, collections.Iterable):
        if isinstance(obj, dict):
            obj = sorted(list(obj.values()))
        if isinstance(obj, str):
            print(obj)
            yield obj
            return
        for obj_ in obj:
            for o in full_traverse(obj_):
                yield o
    else:
        yield obj

def _pprint(params, offset=0, printer=repr):
    # Do a multi-line justified repr:
    options = np.get_printoptions()
    np.set_printoptions(precision=5, threshold=64, edgeitems=2)
    params_list = list()
    this_line_length = offset
    line_sep = ',\n' + (1 + offset) * ' '
    for i, (k, v) in enumerate(params):
        if type(v) is float:
            # use str for representing floating point numbers
            # this way we get consistent representation across
            # architectures and versions.
            this_repr = '%s=%s' % (k, str(v))
        else:
            # use repr of the rest
            this_repr = '%s=%s' % (k, printer(v))
        if len(this_repr) > 500:
            this_repr = this_repr[:300] + '...' + this_repr[-100:]
        if i > 0:
            if (this_line_length + len(this_repr) >= 75 or '\n' in this_repr):
                params_list.append(line_sep)
                this_line_length = len(line_sep)
            else:
                params_list.append(', ')
                this_line_length += 2
        params_list.append(this_repr)
        this_line_length += len(this_repr)

    np.set_printoptions(**options)
    lines = ''.join(params_list)
    # Strip trailing space to avoid nightmare in doctests
    lines = '\n'.join(l.rstrip(' ') for l in lines.split('\n'))
    return lines

class Task:
    tasks = []

    def __init__(self, *args, **kwargs):
        inputs = self.get_inputs()
        input_values = self.get_input_values(inputs, args, kwargs)
        # Set all values on class instance
        for key, value in input_values:
            setattr(self, key, value)
        # Register args and kwargs as an attribute on the class. Might be useful
        self.input_values = input_values
        self.input_args = tuple(value for key, value in input_values)

        self.user_run = self.run

        # Add custom modifications to output()
        self.user_outputs = self.outputs
        cached_outputs = self.user_outputs()

        for i, outp in enumerate(full_traverse(cached_outputs)):
            outp.unique_key = i
            if not outp.parent is None:
                raise ValueError(
                        'Target {} produced by both '
                        '{} and {}'.format(outp, outp.parent, self))
            outp.parent = self

        def outputs(self):
            return cached_outputs

        self.outputs = types.MethodType(outputs, self)

        # Add custom modifications to run()
        # TODO move the logging and output checking somewhere else
        def run(self):
            logger.info('RUNNING {}'.format(self))
            self.user_run()
            out = self.outputs()
            if not isinstance(out, collections.Iterable):
                out = [out]
            for o in out:
                if not o.exists():
                    raise RuntimeError('Output {} not created after running task!'.format(o))

            logger.info('FINISHED {}'.format(self))

        self.run = types.MethodType(run, self)

        self.input_args = tuple(value for key, value in input_values)

        self.__class__.tasks.append(self)

    def __repr__(self):
        class_name = self.__class__.__name__
        return '{}({})'.format(class_name, _pprint(self.input_values,
                                                   offset=len(class_name)))

    def inputs(self):
        return self.input_args

    def outputs(self):
        pass

    def run(self):
        pass

    @classmethod
    def get_inputs(cls):
        inputs = []
        for input_name in dir(cls):
            input_obj = getattr(cls, input_name)
            if not isinstance(input_obj, Input):
                continue

            inputs.append((input_name, input_obj))

        inputs.sort(key=lambda t: t[1]._counter)
        return inputs

    @classmethod
    def get_input_values(cls, inputs, args, kwargs):
        result = {}

        inputs_dict = dict(inputs)

        # positional arguments
        positional_inputs = [(n, p) for n, p in inputs]
        for i, arg in enumerate(args):
            if i >= len(positional_inputs):
                raise ValueError()
            input_name, input_obj = positional_inputs[i]
            result[input_name] = arg

        # optional arguments
        for input_name, arg in six.iteritems(kwargs):
            if input_name in result:
                raise ValueError()
            if input_name not in inputs_dict:
                raise ValueError()
            result[input_name] = arg

        # substitute defaults
        for input_name, input_obj in inputs:
            if input_name not in result:
                if input_obj.default is None:
                    raise ValueError("Input '{}' has no default and no value was provided.".format(input_name))
                result[input_name] = input_obj.default

        return [(input_name, result[input_name]) for input_name, input_obj in inputs]

    def __hash__(self):
        return hash((self.input_args,
                     '\n'.join(inspect.getsourcelines(self.user_outputs)[0]),
                     '\n'.join(inspect.getsourcelines(self.user_run)[0]),
                   ))

    def __eq__(self, other):
        return repr(self) == repr(other)

current_task = None

def get_current_task():
    global current_task
    return current_task

