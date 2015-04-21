import dill
import six
import abc
import leveldb
import hashlib
import simplejson

from .log import get_logger
logger = get_logger()

@six.add_metaclass(abc.ABCMeta)
class Target(object):

    targets = []
    known_targets = set()
    db_path = '.datapipe.db'
    db = leveldb.LevelDB(db_path)

    def __init__(self):
        self._parent = ''
        self._checksum = ''
        self._memory = {}

        if not self in self.__class__.known_targets:
            self.__class__.targets.append(self)
            self.__class__.known_targets.add(self)

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

    def store(self, batch=None):
        if batch is None:
            db = self.__class__.db
        else:
            db = batch
        db.Put(self.checksum(), simplejson.dumps(self._memory).encode('utf-8'))

    def stored(self):
        cls = self.__class__
        try:
            data = cls.db.Get(self.checksum())
            return simplejson.loads(data.decode('utf-8'))
        except KeyError:
            return None

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



