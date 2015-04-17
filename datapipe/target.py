import dill

from .log import get_logger
logger = get_logger()

class Target(object):
    def __init__(self):
        self.parent = None
        self.unique_key = None
        self.force_update = False

    def exists(self):
        return True

    def get(self):
        pass

    def set_unique(self, key):
        self.unique_key = key

    def __repr__(self):
        return self.__class__.__name__ + '(...)'

    def __hash__(self):
        """
        The hash of a target depends on

         - The hash of its creating task
           (i.e. all targets needed to produce it)
         - The representation, if the target exists
         - A key that is unique for every output
           of its creating task
        
        If the hash changes, we definitely want to recompute the target.
        Other conditions for recomputing can be added through subclassing.
        """
        if self.exists():
            hsh = hash(dill.dumps(self.get()))
        else:
            hsh = 0
        return hash((hsh, self.parent, self.unique_key))
    
    def has_changed(self, other):
        """
        Decides if this target has changed
        """
        return hash(self) != hash(other)

