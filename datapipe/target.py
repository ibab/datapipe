import dill
import six
import abc
import sqlite3
import hashlib

from .log import get_logger
logger = get_logger()

@six.add_metaclass(abc.ABCMeta)
class Target(object):

    targets = []
    db_path = '.datapipe.db'
    conn = sqlite3.connect(db_path)
    conn.text_factory = str
    c = conn.cursor()

    def __init__(self):
        self._parent = ''
        self.force_update = False
        self._hash = 0
        self._checksum = ''

        if not self in self.__class__.targets:
            self.targets.append(self)

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
        data = cls.c.execute('SELECT * FROM state WHERE hash=?', (self.checksum(),)).fetchone()
        if data:
            return dill.loads(data[2])
        else:
            return None

    def store(self):
        cls = self.__class__
        entry = cls.c.execute('SELECT * FROM state WHERE hash=?', (self.checksum(),)).fetchone()
        data = dill.dumps(self)
        if entry:
            cls.c.execute('UPDATE state SET data=? WHERE hash=?', (data, self.checksum()))
        else:
            cls.c.execute('INSERT INTO state (hash, data) VALUES (?, ?)', (self.checksum(), data))
        cls.conn.commit()

    @classmethod
    def create_store(cls):
        cls.c.execute('CREATE TABLE IF NOT EXISTS state (id integer primary key, hash blob, data blob)')

    @classmethod
    def clear_store(cls):
        cls.c.execute('DROP TABLE IF EXISTS state')
        cls.conn.commit()
        cls.create_store()

    def checksum(self):
        if self._checksum:
            return self._checksum
        else:
            m = hashlib.sha1()
            m.update(self.__class__.__name__.encode())
            if self._parent:
                m.update(self._parent.checksum().encode())
            m.update(self.identifier().encode())
            self._checksum = m.hexdigest()
            return m.hexdigest()

    def __hash__(self):
        if not self._hash:
            self._hash = hash((self.__class__.__name__, self._parent, self.identifier()))
        return self._hash

    def __eq__(self, other):
        return all([self.__class__.__name__ == other.__class__.__name__,
                    self._parent == other._parent,
                    self.identifier() == other.identifier()])


