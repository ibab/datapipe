from .task import get_current_task

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

