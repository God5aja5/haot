import json
import os
import sqlite3
import threading


class StatsStore:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS users ("
                "user_id INTEGER PRIMARY KEY"
                ")"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS stats ("
                "id INTEGER PRIMARY KEY CHECK (id = 1),"
                "total_lines_checked INTEGER NOT NULL DEFAULT 0,"
                "total_hits INTEGER NOT NULL DEFAULT 0"
                ")"
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO stats (id, total_lines_checked, total_hits) "
                "VALUES (1, 0, 0)"
            )

    def add_user(self, user_id):
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO users (user_id) VALUES (?)",
                (int(user_id),),
            )

    def add_run(self, lines_checked, hits):
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE stats SET total_lines_checked = total_lines_checked + ?, "
                "total_hits = total_hits + ? WHERE id = 1",
                (int(lines_checked), int(hits)),
            )

    def snapshot(self):
        with self.lock:
            cur = self.conn.execute(
                "SELECT total_lines_checked, total_hits FROM stats WHERE id = 1"
            )
            row = cur.fetchone() or (0, 0)
            cur = self.conn.execute("SELECT COUNT(1) FROM users")
            total_users = cur.fetchone()[0]
            return {
                "total_users": int(total_users),
                "total_lines_checked": int(row[0]),
                "total_hits": int(row[1]),
            }


class UsersStore:
    def __init__(self, db_path, json_path=None):
        self.db_path = db_path
        self.json_path = json_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_db()
        self._export_json()

    def _init_db(self):
        with self.conn:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS users ("
                "user_id INTEGER PRIMARY KEY"
                ")"
            )

    def _list_users_unlocked(self):
        cur = self.conn.execute("SELECT user_id FROM users ORDER BY user_id")
        return [row[0] for row in cur.fetchall()]

    def _export_json(self):
        if not self.json_path:
            return
        users = self.list_users()
        tmp = self.json_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"users": users}, f, ensure_ascii=True, indent=2)
        os.replace(tmp, self.json_path)

    def export_json(self):
        self._export_json()

    def add_user(self, user_id):
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO users (user_id) VALUES (?)",
                (int(user_id),),
            )
        self._export_json()

    def list_users(self):
        with self.lock:
            return self._list_users_unlocked()
