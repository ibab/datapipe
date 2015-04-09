import six
import types
import sqlite3
import collections

import logging
logger = logging.getLogger('datapipe')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

class Input:
    # Used for calculating the order that Inputs are defined in.
    # The counter increases globally over all instances of Input
    _counter = 0
    def __init__(self, default=None):
        self.default = default
        self._counter = Input._counter
        Input._counter += 1

class Task:

    def __init__(self, *args, **kwargs):
        global current_task
        current_task = self

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
        self.user_output = self.output
        def output(self):
            global current_task
            current_task = self
            outputs = self.user_output()
            current_task = None
            return outputs

        self.output = types.MethodType(output, self)

        # Add custom modifications to run()
        def run(self):
            logger.info('RUNNING {}'.format(self))
            self.user_run()
            out = self.output()
            if isinstance(out, collections.Iterable):
                for o in out:
                    if not o.exists():
                        raise RuntimeError('Output {} not created after running task!'.format(o))
            else:
                if not out.exists():
                    raise RuntimeError('Output {} not created after running task!'.format(out))

            logger.info('FINISHED {}'.format(self))

        self.run = types.MethodType(run, self)

        self.input_args = tuple(value for key, value in input_values)

        current_task = None

    def __str__(self):
        result = []
        for k, w in self.input_values:
            result.append('{}={}'.format(k, repr(w)))

        return self.__class__.__name__ + '(' + ', '.join(result) + ')'

    def output(self):
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
                    raise ValueError()
                result[input_name] = input_obj.default

        return [(input_name, result[input_name]) for input_name, input_obj in inputs]

class History:
    def __init__(self):
        self.conn = sqlite3.connect('.history.db')

def get_current_task():
    global current_task
    return current_task

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

current_task = None

