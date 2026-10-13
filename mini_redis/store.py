"""In-memory key-value data store for Mini-Redis.

Kept completely separate from network/protocol logic.
Supports an injectable clock function for deterministic testing (Phase 3 TTL).
"""

import time
from typing import Callable, Optional


class Store:
    """
    In-memory key-value store mapping bytes -> bytes.
    """

    def __init__(self, clock: Optional[Callable[[], float]] = None) -> None:
        """
        Initialize the Store.

        Args:
            clock: A callable returning current time in seconds (defaults to time.monotonic).
        """
        self._clock: Callable[[], float] = clock or time.monotonic
        self._data: dict[bytes, bytes] = {}

    @property
    def clock(self) -> Callable[[], float]:
        """Return the store's clock function."""
        return self._clock

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Retrieve value for key.

        Returns:
            The value bytes, or None if key does not exist.
        """
        return self._data.get(key)

    def set(self, key: bytes, value: bytes) -> None:
        """
        Set key to value.
        """
        self._data[key] = value

    def delete(self, *keys: bytes) -> int:
        """
        Delete one or more keys.

        Returns:
            The number of keys that were removed.
        """
        count = 0
        for key in keys:
            if key in self._data:
                del self._data[key]
                count += 1
        return count

    def exists(self, *keys: bytes) -> int:
        """
        Check existence of one or more keys.

        Returns:
            The number of keys that exist in the store.
        """
        count = 0
        for key in keys:
            if key in self._data:
                count += 1
        return count

    def flush(self) -> None:
        """Clear all keys in store."""
        self._data.clear()

    def __len__(self) -> int:
        return len(self._data)
