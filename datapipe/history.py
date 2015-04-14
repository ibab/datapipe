import sqlite3
import tempfile

from .log import get_logger
import datapipe.target
logger = get_logger()

class History:
    def __init__(self, path):
        self.path = path

        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS state (id integer primary key, hash integer, timestamp double, path string)')
        conn.commit()
        c.close()
        conn.close()

    def target_changed(self, target):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        if not target.exists():
            return True
        ts = target.timestamp()
        c.execute('SELECT * FROM state WHERE hash=?', (hash(target),))
        data = c.fetchone()
        if data and ts:
            k = ts > data[2]
            if k:
                return True
            else:
                return False
        elif data:
            return False
        else:
            return True
        c.close()
        conn.close()

    def get_target(self, target):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        hsh = hash(target)
        ret = c.execute('SELECT * FROM state WHERE hash=?', (hsh,)).fetchone()
        c.close()
        conn.close()
        return ret

    def add_target(self, trgt):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        ts = trgt.timestamp()
        hsh = hash(trgt)

        entry = c.execute('SELECT * FROM state WHERE hash=?', (hsh,)).fetchone()
        if entry:
            c.execute('UPDATE state SET timestamp=?, path=? WHERE hash=?', (ts, entry[3], hsh))
        else:
            if isinstance(trgt, datapipe.target.PyTarget):
                pth = tempfile.NamedTemporaryFile(delete=False).name
            else:
                pth = ""
            c.execute('INSERT INTO state (hash, timestamp, path) VALUES (?, ?, ?)', (hsh, ts, pth))
        c.close()
        conn.commit()
        conn.close()

