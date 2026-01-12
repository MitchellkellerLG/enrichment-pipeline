"""
SQLite-based cache store with TTL support for enrichment pipeline artifacts.

Provides persistent caching with configurable TTL per artifact type,
thread-safe operations, and cost tracking for API call savings.
"""

import json
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional


# TTL configuration in seconds per artifact type
CACHE_TTL: dict[str, int] = {
    "icp_scrape": 14 * 24 * 3600,      # 14 days
    "careers_scrape": 3 * 24 * 3600,   # 3 days
    "news_search": 12 * 3600,          # 12 hours
    "mx_check": 30 * 24 * 3600,        # 30 days
    "linkedin_url": 90 * 24 * 3600,    # 90 days
}

DEFAULT_TTL: int = 7 * 24 * 3600  # 7 days for unknown types


class CacheStore:
    """
    Thread-safe SQLite cache with TTL support and cost tracking.

    Stores enrichment artifacts with automatic expiration based on
    artifact type. Tracks API costs to measure savings from cache hits.

    Attributes:
        db_path: Path to the SQLite database file.

    Example:
        cache = CacheStore()
        cache.set("icp_scrape", "example.com", {"industry": "tech"}, cost=0.05)
        result = cache.get("icp_scrape", "example.com")
    """

    def __init__(self, db_path: str = ".state/cache.db") -> None:
        """
        Initialize the cache store with SQLite backend.

        Creates the database file and cache table if they don't exist.
        Sets up thread-safe access via threading Lock.

        Args:
            db_path: Path to the SQLite database file.
                     Defaults to ".state/cache.db".
        """
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize database schema
        self._init_db()

    def _init_db(self) -> None:
        """Create cache table and indexes if they don't exist."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cache (
                        cache_key TEXT PRIMARY KEY,
                        artifact_type TEXT NOT NULL,
                        value_json TEXT NOT NULL,
                        cost_usd REAL DEFAULT 0,
                        created_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL
                    )
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cache_expires_at
                    ON cache(expires_at)
                """)
                conn.commit()
            finally:
                conn.close()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a new database connection."""
        return sqlite3.connect(str(self.db_path))

    def _make_cache_key(self, artifact_type: str, key: str) -> str:
        """
        Create a composite cache key.

        Args:
            artifact_type: Type of artifact (e.g., "icp_scrape").
            key: Unique identifier (e.g., domain name).

        Returns:
            Composite key in format "artifact_type:key".
        """
        return f"{artifact_type}:{key}"

    def _get_ttl(self, artifact_type: str) -> int:
        """
        Get TTL in seconds for an artifact type.

        Args:
            artifact_type: Type of artifact.

        Returns:
            TTL in seconds from CACHE_TTL or DEFAULT_TTL.
        """
        return CACHE_TTL.get(artifact_type, DEFAULT_TTL)

    def get(self, artifact_type: str, key: str) -> Optional[dict]:
        """
        Retrieve a cached value if it exists and hasn't expired.

        Args:
            artifact_type: Type of artifact (e.g., "icp_scrape").
            key: Unique identifier (e.g., domain name).

        Returns:
            Cached dictionary value if found and not expired, None otherwise.

        Example:
            result = cache.get("icp_scrape", "example.com")
            if result:
                print(f"Cache hit: {result}")
        """
        cache_key = self._make_cache_key(artifact_type, key)
        now = datetime.utcnow().isoformat()

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT value_json FROM cache
                    WHERE cache_key = ? AND expires_at > ?
                """, (cache_key, now))
                row = cursor.fetchone()

                if row:
                    self._hits += 1
                    return json.loads(row[0])
                else:
                    self._misses += 1
                    return None
            except (sqlite3.Error, json.JSONDecodeError) as e:
                self._misses += 1
                return None
            finally:
                conn.close()

    def set(
        self,
        artifact_type: str,
        key: str,
        value: dict,
        cost: float = 0.0
    ) -> None:
        """
        Store a value in the cache with automatic TTL.

        TTL is determined by artifact_type from CACHE_TTL configuration.

        Args:
            artifact_type: Type of artifact (e.g., "icp_scrape").
            key: Unique identifier (e.g., domain name).
            value: Dictionary value to cache.
            cost: API cost in USD for this lookup (for tracking savings).

        Example:
            cache.set("icp_scrape", "example.com", {"industry": "tech"}, cost=0.05)
        """
        cache_key = self._make_cache_key(artifact_type, key)
        ttl_seconds = self._get_ttl(artifact_type)

        now = datetime.utcnow()
        created_at = now.isoformat()
        expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
        value_json = json.dumps(value)

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO cache
                    (cache_key, artifact_type, value_json, cost_usd, created_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (cache_key, artifact_type, value_json, cost, created_at, expires_at))
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                raise RuntimeError(f"Cache set failed: {e}") from e
            finally:
                conn.close()

    def invalidate(self, artifact_type: str, key: str) -> bool:
        """
        Manually invalidate a cache entry.

        Args:
            artifact_type: Type of artifact.
            key: Unique identifier.

        Returns:
            True if an entry was deleted, False if not found.

        Example:
            cache.invalidate("icp_scrape", "example.com")
        """
        cache_key = self._make_cache_key(artifact_type, key)

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM cache WHERE cache_key = ?", (cache_key,))
                deleted = cursor.rowcount > 0
                conn.commit()
                return deleted
            except sqlite3.Error as e:
                conn.rollback()
                raise RuntimeError(f"Cache invalidate failed: {e}") from e
            finally:
                conn.close()

    def cleanup_expired(self) -> int:
        """
        Remove all expired cache entries.

        Should be called periodically to reclaim disk space.

        Returns:
            Number of expired entries removed.

        Example:
            removed = cache.cleanup_expired()
            print(f"Cleaned up {removed} expired entries")
        """
        now = datetime.utcnow().isoformat()

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM cache WHERE expires_at <= ?", (now,))
                deleted = cursor.rowcount
                conn.commit()
                return deleted
            except sqlite3.Error as e:
                conn.rollback()
                raise RuntimeError(f"Cache cleanup failed: {e}") from e
            finally:
                conn.close()

    def get_stats(self) -> dict[str, Any]:
        """
        Get cache statistics including hit/miss rates and cost savings.

        Returns:
            Dictionary containing:
                - hits: Number of cache hits in this session
                - misses: Number of cache misses in this session
                - hit_rate: Percentage of hits (0-100)
                - total_entries: Total entries currently in cache
                - valid_entries: Entries that haven't expired
                - expired_entries: Entries pending cleanup
                - total_cost_saved: Sum of cost_usd for all valid entries
                - cost_by_type: Cost breakdown by artifact type

        Example:
            stats = cache.get_stats()
            print(f"Hit rate: {stats['hit_rate']:.1f}%")
            print(f"Cost saved: ${stats['total_cost_saved']:.2f}")
        """
        now = datetime.utcnow().isoformat()

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Total entries
                cursor.execute("SELECT COUNT(*) FROM cache")
                total_entries = cursor.fetchone()[0]

                # Valid (non-expired) entries
                cursor.execute(
                    "SELECT COUNT(*) FROM cache WHERE expires_at > ?",
                    (now,)
                )
                valid_entries = cursor.fetchone()[0]

                # Total cost saved (from valid entries only)
                cursor.execute(
                    "SELECT COALESCE(SUM(cost_usd), 0) FROM cache WHERE expires_at > ?",
                    (now,)
                )
                total_cost_saved = cursor.fetchone()[0]

                # Cost breakdown by type
                cursor.execute("""
                    SELECT artifact_type, SUM(cost_usd), COUNT(*)
                    FROM cache
                    WHERE expires_at > ?
                    GROUP BY artifact_type
                """, (now,))
                cost_by_type = {
                    row[0]: {"cost": row[1], "entries": row[2]}
                    for row in cursor.fetchall()
                }

                # Calculate hit rate
                total_requests = self._hits + self._misses
                hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0.0

                return {
                    "hits": self._hits,
                    "misses": self._misses,
                    "hit_rate": hit_rate,
                    "total_entries": total_entries,
                    "valid_entries": valid_entries,
                    "expired_entries": total_entries - valid_entries,
                    "total_cost_saved": total_cost_saved,
                    "cost_by_type": cost_by_type,
                }
            except sqlite3.Error as e:
                raise RuntimeError(f"Failed to get cache stats: {e}") from e
            finally:
                conn.close()
