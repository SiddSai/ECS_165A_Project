import threading

LOCK_SHARED = "S"
LOCK_EXCLUSIVE = "X"


class LockManager:
    """
    Keeps track of who is locking which record.

    Two types of locks:
      - Shared lock (read): multiple transactions can hold this at the same time
      - Exclusive lock (write): only one transaction can hold this, nobody else can read or write

    No-Wait: if the lock you want is taken, we return False right away instead of waiting.
    The transaction will abort and retry later. This way deadlocks can never happen.
    """

    def __init__(self):
        # rid -> {"holders": {txn_id: lock_type}, "exclusive_holder": txn_id or None}
        self._lock_table = {}
        self._mutex = threading.Lock()  # protects the lock table from concurrent access

    def acquire_shared(self, rid, txn_id):
        """
        Try to get a read lock on this record.
        Returns True if successful, False if someone else is writing (No-Wait).
        """
        with self._mutex:
            entry = self._lock_table.get(rid)
            if entry is None:
                self._lock_table[rid] = {"holders": {txn_id: LOCK_SHARED}, "exclusive_holder": None}
                return True

            holders = entry["holders"]
            exclusive_holder = entry["exclusive_holder"]

            # already holding a lock on this record, no need to do anything
            if txn_id in holders:
                return True

            # someone else is writing, we can't read right now
            if exclusive_holder is not None and exclusive_holder != txn_id:
                return False

            holders[txn_id] = LOCK_SHARED
            return True

    def acquire_exclusive(self, rid, txn_id):
        """
        Try to get a write lock on this record.
        Returns True if successful, False if anyone else has any lock (No-Wait).
        Also handles upgrading from a read lock to a write lock if we're the only reader.
        """
        with self._mutex:
            entry = self._lock_table.get(rid)
            if entry is None:
                self._lock_table[rid] = {"holders": {txn_id: LOCK_EXCLUSIVE}, "exclusive_holder": txn_id}
                return True

            holders = entry["holders"]
            exclusive_holder = entry["exclusive_holder"]

            # already have the write lock
            if exclusive_holder == txn_id:
                return True

            # someone else is writing
            if exclusive_holder is not None:
                return False

            # someone else is reading, we can't write
            other_shared = [t for t in holders if t != txn_id]
            if other_shared:
                return False

            holders[txn_id] = LOCK_EXCLUSIVE
            entry["exclusive_holder"] = txn_id
            return True

    def release_all(self, txn_id):
        """Drop all locks held by this transaction. Called when a transaction commits or aborts."""
        with self._mutex:
            to_clean = []
            for rid, entry in self._lock_table.items():
                if txn_id in entry["holders"]:
                    del entry["holders"][txn_id]
                    if entry["exclusive_holder"] == txn_id:
                        entry["exclusive_holder"] = None
                if not entry["holders"]:
                    to_clean.append(rid)
            for rid in to_clean:
                del self._lock_table[rid]

    def reset(self):
        """Clear everything. Useful between test runs."""
        with self._mutex:
            self._lock_table.clear()


_global_lock_manager = LockManager()


def get_lock_manager():
    return _global_lock_manager


def reset_lock_manager():
    """Reset the global lock manager between tests."""
    _global_lock_manager.reset()