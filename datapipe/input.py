
class Input:
    # Used for calculating the order that Inputs are defined in.
    # The counter increases globally over all instances of Input
    _counter = 0
    def __init__(self, default=None):
        self.default = default
        self._counter = Input._counter
        Input._counter += 1

