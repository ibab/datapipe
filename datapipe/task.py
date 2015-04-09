import six
import types
import logging
import collections

from .input import Input

logger = logging.getLogger('datapipe')

class Task:

    tasks = []

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
        self.user_outputs = self.outputs
        def outputs(self):
            global current_task
            current_task = self
            outputs = self.user_outputs()
            current_task = None
            return outputs

        self.outputs = types.MethodType(outputs, self)

        # Add custom modifications to run()
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

        current_task = None

        self.__class__.tasks.append(self)

    def __repr__(self):
        result = []
        for k, w in self.input_values:
            result.append('{}={}'.format(k, repr(w)))

        return self.__class__.__name__ + '(' + ', '.join(result) + ')'

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
                    raise ValueError()
                result[input_name] = input_obj.default

        return [(input_name, result[input_name]) for input_name, input_obj in inputs]

current_task = None

def get_current_task():
    global current_task
    return current_task

