import sqlite3

class History:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)

