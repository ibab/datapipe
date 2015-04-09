import six
import types

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
        params = self.get_params()
        param_values = self.get_param_values(params, args, kwargs)
        # Set all values on class instance
        for key, value in param_values:
            setattr(self, key, value)
        # Register args and kwargs as an attribute on the class. Might be useful
        self.param_values = param_values
        self.param_args = tuple(value for key, value in param_values)

        self.user_run = self.run

        def run(self):
            logger.info('RUNNING {}'.format(self))
            self.user_run()
            logger.info('FINISHED {}'.format(self))

        # Register args and kwargs as an attribute on the class. Might be useful
        self.param_args = tuple(value for key, value in param_values)

        self.run = types.MethodType(run, self)

    def __str__(self):
        result = []
        for k, w in self.param_values:
            result.append('{}={}'.format(k, repr(w)))

        return self.__class__.__name__ + '(' + ', '.join(result) + ')'

    def output(self):
        pass

    def run(self):
        pass

    @classmethod
    def get_params(cls):
        params = []
        for param_name in dir(cls):
            param_obj = getattr(cls, param_name)
            if not isinstance(param_obj, Input):
                continue

            params.append((param_name, param_obj))

        params.sort(key=lambda t: t[1]._counter)
        return params

    @classmethod
    def get_param_values(cls, params, args, kwargs):
        result = {}

        params_dict = dict(params)

        # positional arguments
        positional_params = [(n, p) for n, p in params]
        for i, arg in enumerate(args):
            if i >= len(positional_params):
                raise ValueError()
            param_name, param_obj = positional_params[i]
            result[param_name] = arg

        # optional arguments
        for param_name, arg in six.iteritems(kwargs):
            if param_name in result:
                raise ValueError()
            if param_name not in params_dict:
                raise ValueError()
            result[param_name] = arg

        # substitute defaults
        for param_name, param_obj in params:
            if param_name not in result:
                if param_obj.default is None:
                    raise ValueError()
                result[param_name] = param_obj.default

        return [(param_name, result[param_name]) for param_name, param_obj in params]

class Target:
    def __init__(self):
        pass

class LocalFile(Target):
    def __init__(self, path):
        self.path = path

    def clone(self, path=None):
        if not path is None:
            return LocalFile(path)
        else:
            return LocalFile(self.path)
    
    def get(self):
        return self.path

    def from_suffix(self, suf, app):
        return self.clone(path=self.path.replace(suf, app))

    def __repr__(self):
        return "LocalFile('" + self.path + "')"


