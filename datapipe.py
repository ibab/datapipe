import six

class Input:
    # Used for calculating the order that Inputs are defined in.
    # The counter increases globally over all instances of Input
    counter = 0
    def __init__(self, default=None):
        self.default = default
        self.counter = Input.counter
        Input.counter += 1

class Task:

    def __init__(self, *args, **kwargs):
        params = self.get_params()
        param_values = self.get_param_values(params, args, kwargs)
        # Set all values on class instance
        for key, value in param_values:
            setattr(self, key, value)
        # Register args and kwargs as an attribute on the class. Might be useful
        self.param_args = tuple(value for key, value in param_values)
        self.param_kwargs = dict(param_values)

    def input(self):
        pass

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

        params.sort(key=lambda t: t[1].counter)
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

        for param_name, param_obj in params:
            if param_name not in result:
                if param_obj.default is None:
                    raise ValueError()
                result[param_name] = param_obj.default

        return [(param_name, result[param_name]) for param_name, param_obj in params]

class Target:
    def __init__(self):
        pass

class FilePath(Target):
    def __init__(self, path):
        self.path = path

    def clone(self, path=None):
        if not path is None:
            return FilePath(path)
        else:
            return FilePath(self.path)
    
    def get(self):
        return self.path

    def from_suffix(self, suf, app):
        return self.clone(path=self.path.replace(suf, app))


