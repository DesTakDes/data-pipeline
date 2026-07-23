"""
preview.node_cache
────────────────────
Caches a compiled node's Spark DataFrame so repeated Preview clicks on the
same node (with unchanged upstream config) don't re-scan Postgres via JDBC
every time. Key includes the FULL ancestor config chain — if any upstream
node's config changes, the cache key changes too, so stale results are
never served.
"""
import hashlib
import json
import time


class NodeResultCache:
    def __init__(self, ttl_seconds: int = 300, max_entries: int = 50):
        self._store: dict[str, tuple[float, object]] = {}
        self.ttl = ttl_seconds
        self.max_entries = max_entries

    @staticmethod
    def make_key(node_id: str, ancestor_chain: list[dict]) -> str:
        payload = json.dumps({"node_id": node_id, "chain": ancestor_chain}, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    def get(self, key: str):
        entry = self._store.get(key)
        if not entry:
            return None
        ts, df = entry
        if time.time() - ts > self.ttl:
            del self._store[key]
            return None
        return df

    def put(self, key: str, df):
        if len(self._store) >= self.max_entries:
            oldest_key = min(self._store, key=lambda k: self._store[k][0])
            del self._store[oldest_key]
        self._store[key] = (time.time(), df)

    def clear(self):
        self._store.clear()


# Module-level singleton — shared across requests within the same worker process.
default_cache = NodeResultCache()