import sqlite3

from .log import get_logger
logger = get_logger()

class History:
    def __init__(self, path):
        self.path = path

        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS state (id integer primary key, name string, timestamp double, hash integer)')
        conn.commit()
        c.close()
        conn.close()

    def target_changed(self, target):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        name = repr(target)
        if not target.exists():
            return True
        ts = target.timestamp()
        c.execute('SELECT * FROM state WHERE name=?', (name,))
        data = c.fetchone()
        if data:
            if ts:
                return ts > data[2]
            else:
                return hash(target) == data[3]
        else:
            return True
        c.close()
        conn.close()

    def add_target(self, target):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        name = repr(target)
        ts = target.timestamp()
        if ts:
            hsh = 0
        else:
            hsh = hash(target)
        if c.execute('SELECT * FROM state WHERE name=?', (name,)).fetchone():
            c.execute('UPDATE state SET timestamp=?, hash=? WHERE name=?', (ts, hsh, name))
        else:
            c.execute('INSERT INTO state (name, timestamp, hash) VALUES (?, ?, ?)', (name, ts, hsh))
        c.close()
        conn.commit()
        conn.close()

