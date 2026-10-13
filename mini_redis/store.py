"""In-memory key-value data store for Mini-Redis with TTL support.

Kept completely separate from network/protocol logic.
Uses wall-clock milliseconds by default (int(time.time() * 1000)) so absolute
expiry timestamps can be persisted in Phase 4 AOF.
Accepts an injectable clock function for deterministic tests.
"""

import random
import time
from typing import Callable, Optional


def _default_wall_clock_ms() -> int:
    """Return current wall-clock time in milliseconds."""
    return int(time.time() * 1000)


class Store:
    """
    In-memory key-value store mapping bytes -> bytes with per-key TTL.
    """

    def __init__(self, clock: Optional[Callable[[], int]] = None) -> None:
        """
        Initialize the Store.

        Args:
            clock: A callable returning current time in milliseconds
                   (defaults to wall-clock milliseconds: int(time.time() * 1000)).
        """
        self._clock: Callable[[], int] = clock or _default_wall_clock_ms
        self._data: dict[bytes, bytes] = {}
        self._expires: dict[bytes, int] = {}  # key -> absolute expiry in ms

    @property
    def clock(self) -> Callable[[], int]:
        """Return the store's clock function."""
        return self._clock

    def _check_expired(self, key: bytes) -> bool:
        """
        Check if a key has expired. If expired, lazily delete it.

        Returns:
            True if the key was expired and removed, False otherwise.
        """
        exp = self._expires.get(key)
        if exp is not None and self._clock() >= exp:
            self._expires.pop(key, None)
            self._data.pop(key, None)
            return True
        return False

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Retrieve value for key. Lazily expires keys on read.

        Returns:
            The value bytes, or None if key does not exist or has expired.
        """
        if self._check_expired(key):
            return None
        return self._data.get(key)

    def set(
        self, key: bytes, value: bytes, expire_ms: Optional[int] = None
    ) -> None:
        """
        Set key to value. Overwriting or plain SET clears any previous TTL.

        Args:
            key: Key as bytes.
            value: Value as bytes.
            expire_ms: Optional absolute expiry timestamp in milliseconds.
        """
        self._data[key] = value
        if expire_ms is not None:
            self._expires[key] = expire_ms
        else:
            self._expires.pop(key, None)

    def delete(self, *keys: bytes) -> int:
        """
        Delete one or more keys. Clearing a key also removes its expiry.

        Returns:
            The number of keys that were removed.
        """
        count = 0
        for key in keys:
            if self._check_expired(key):
                continue
            self._expires.pop(key, None)
            if key in self._data:
                del self._data[key]
                count += 1
        return count

    def exists(self, *keys: bytes) -> int:
        """
        Check existence of one or more keys. Lazily expires keys on check.

        Returns:
            The number of keys that exist and have not expired.
        """
        count = 0
        for key in keys:
            if not self._check_expired(key) and key in self._data:
                count += 1
        return count

    def expire(self, key: bytes, seconds: int) -> int:
        """
        Set TTL in seconds for key.

        Returns:
            1 if the timeout was set, 0 if key does not exist or has expired.
        """
        if self._check_expired(key) or key not in self._data:
            return 0
        if seconds <= 0:
            self.delete(key)
            return 1
        self._expires[key] = self._clock() + seconds * 1000
        return 1

    def pexpire(self, key: bytes, milliseconds: int) -> int:
        """
        Set TTL in milliseconds for key.

        Returns:
            1 if the timeout was set, 0 if key does not exist or has expired.
        """
        if self._check_expired(key) or key not in self._data:
            return 0
        if milliseconds <= 0:
            self.delete(key)
            return 1
        self._expires[key] = self._clock() + milliseconds
        return 1

    def pexpireat(self, key: bytes, timestamp_ms: int) -> int:
        """
        Set absolute expiry in milliseconds for key.

        Returns:
            1 if the timeout was set, 0 if key does not exist or has expired.
        """
        if self._check_expired(key) or key not in self._data:
            return 0
        if self._clock() >= timestamp_ms:
            self.delete(key)
            return 1
        self._expires[key] = timestamp_ms
        return 1

    def ttl(self, key: bytes) -> int:
        """
        Get remaining TTL in seconds.

        Returns:
            -2 if the key does not exist (or expired),
            -1 if the key exists but has no expiry,
            seconds remaining (>= 0).
        """
        if self._check_expired(key) or key not in self._data:
            return -2
        if key not in self._expires:
            return -1

        remaining_ms = self._expires[key] - self._clock()
        if remaining_ms <= 0:
            self.delete(key)
            return -2

        # Round to nearest second, minimum 1 second if not expired
        ttl_sec = (remaining_ms + 500) // 1000
        return max(1, ttl_sec)

    def pttl(self, key: bytes) -> int:
        """
        Get remaining TTL in milliseconds.

        Returns:
            -2 if the key does not exist (or expired),
            -1 if the key exists but has no expiry,
            milliseconds remaining (>= 0).
        """
        if self._check_expired(key) or key not in self._data:
            return -2
        if key not in self._expires:
            return -1

        remaining_ms = self._expires[key] - self._clock()
        if remaining_ms <= 0:
            self.delete(key)
            return -2
        return remaining_ms

    def persist(self, key: bytes) -> int:
        """
        Remove the existing timeout on key.

        Returns:
            1 if timeout was removed, 0 if key does not exist or had no expiry.
        """
        if self._check_expired(key) or key not in self._data:
            return 0
        if key in self._expires:
            del self._expires[key]
            return 1
        return 0

    def get_expiry(self, key: bytes) -> Optional[int]:
        """
        Return a key's absolute expiry in milliseconds, or None if no TTL.
        Helper for Phase 4 AOF persistence logging.
        """
        if self._check_expired(key) or key not in self._data:
            return None
        return self._expires.get(key)

    def active_expire_sweep(
        self, sample_size: int = 20, threshold_ratio: float = 0.25, max_loops: int = 10
    ) -> int:
        """
        Active expiration algorithm (matches Redis):
        Sample up to `sample_size` keys from `_expires`. Delete expired ones.
        If more than `threshold_ratio` (default 25%) were expired, repeat immediately,
        capped at `max_loops` to prevent starving the event loop.

        Returns:
            Total number of expired keys removed during this sweep.
        """
        total_deleted = 0
        for _ in range(max_loops):
            if not self._expires:
                break

            now = self._clock()
            keys = list(self._expires.keys())
            sample = random.sample(keys, min(len(keys), sample_size))
            if not sample:
                break

            expired_count = 0
            for k in sample:
                exp = self._expires.get(k)
                if exp is not None and now >= exp:
                    self._expires.pop(k, None)
                    self._data.pop(k, None)
                    expired_count += 1

            total_deleted += expired_count

            # If 25% or fewer were expired, stop repeating this cycle
            if (expired_count / len(sample)) <= threshold_ratio:
                break

        return total_deleted

    def flush(self) -> None:
        """Clear all keys and expiries."""
        self._data.clear()
        self._expires.clear()

    def __len__(self) -> int:
        return len(self._data)
