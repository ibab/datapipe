import dill
import six
import abc
import leveldb
import hashlib

from .log import get_logger
logger = get_logger()

@six.add_metaclass(abc.ABCMeta)
class Target(object):

    targets = []
    db_path = '.datapipe.db'
    db = leveldb.LevelDB(db_path)

    def __init__(self):
        self._parent = ''
        self.force_update = False
        self._hash = 0
        self._checksum = ''

        if not self in self.__class__.targets:
            self.__class__.targets.append(self)

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, value):
        if self._parent:
            raise ValueError("Can\'t set parent of Target twice")
        else:
            self._parent = value

    def __repr__(self):
        return self.__class__.__name__ + '(' + repr(self.identifier()) + ')'

    @abc.abstractproperty
    def identifier(self):
        pass
    
    @abc.abstractmethod
    def is_damaged(self):
        '''
        Decide if the target is damaged.
        The target and all targets that depend on it will be recomputed.
        For example, a target could be damaged if:
         - The underlying object does not exist
         - A timestamp has changed on a file
        '''
        pass

    def stored(self):
        cls = self.__class__
        try:
            data = cls.db.Get(self.checksum())
            return dill.loads(data)
        except KeyError:
            return None

    def store(self, batch=None):
        cls = self.__class__
        if batch:
            batch.Put(self.checksum(), dill.dumps(self))
        else:
            cls.db.Put(self.checksum(), dill.dumps(self))

    @classmethod
    def clear_store(cls):
        batch = leveldb.WriteBatch()
        for k in cls.db.RangeIter(include_value=False):
            batch.Delete(k)
        cls.db.Write(batch)

    def checksum(self):
        if not self._checksum:
            m = hashlib.sha1()
            m.update(self.__class__.__name__.encode())
            if self._parent:
                m.update(self._parent.checksum())
            m.update(self.identifier().encode())
            self._checksum = m.digest()
        return self._checksum



