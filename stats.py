import json
import os
import threading


class StatsStore:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.data = {
            "users": [],
            "total_lines_checked": 0,
            "total_hits": 0,
        }
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                self.data.update(data)
        except Exception:
            pass

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=True, indent=2)
        os.replace(tmp, self.path)

    def add_user(self, user_id):
        with self.lock:
            if user_id not in self.data["users"]:
                self.data["users"].append(user_id)
                self._save()

    def add_run(self, lines_checked, hits):
        with self.lock:
            self.data["total_lines_checked"] += int(lines_checked)
            self.data["total_hits"] += int(hits)
            self._save()

    def snapshot(self):
        with self.lock:
            return {
                "total_users": len(self.data.get("users", [])),
                "total_lines_checked": int(self.data.get("total_lines_checked", 0)),
                "total_hits": int(self.data.get("total_hits", 0)),
            }


class UsersStore:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.data = {"users": []}
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("users"), list):
                self.data["users"] = data["users"]
        except Exception:
            pass

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=True, indent=2)
        os.replace(tmp, self.path)

    def add_user(self, user_id):
        with self.lock:
            if user_id not in self.data["users"]:
                self.data["users"].append(user_id)
                self._save()

    def list_users(self):
        with self.lock:
            return list(self.data["users"])
