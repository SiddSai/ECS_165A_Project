import threading

LOCK_SHARED = "S"
LOCK_EXCLUSIVE = "X"


class LockManager:
    """
    Centralized lock manager implementing No-Wait 2PL.

    Each record (RID) can have:
      - Multiple shared (read) locks from different transactions, OR
      - Exactly one exclusive (write) lock.

    No-Wait: if a lock cannot be granted immediately, return False immediately.
    The calling transaction must abort (and may retry later).

    Thread safety: all state is protected by a single mutex.
    """

    def __init__(self):
        # rid -> {"holders": {txn_id: lock_type}, "exclusive_holder": txn_id or None}
        self._lock_table = {}
        self._mutex = threading.Lock()

    def acquire_shared(self, rid, txn_id):
        """
        Acquire a shared (read) lock on `rid` for `txn_id`.
        Returns True on success, False on conflict (No-Wait).
        """
        with self._mutex:
            entry = self._lock_table.get(rid)
            if entry is None:
                self._lock_table[rid] = {"holders": {txn_id: LOCK_SHARED}, "exclusive_holder": None}
                return True

            holders = entry["holders"]
            exclusive_holder = entry["exclusive_holder"]

            # Already holds any lock on this rid — OK
            if txn_id in holders:
                return True

            # Another txn holds exclusive — No-Wait: fail
            if exclusive_holder is not None and exclusive_holder != txn_id:
                return False

            holders[txn_id] = LOCK_SHARED
            return True

    def acquire_exclusive(self, rid, txn_id):
        """
        Acquire an exclusive (write) lock on `rid` for `txn_id`.
        Returns True on success, False on conflict (No-Wait).
        Supports upgrade from shared → exclusive if this txn is the sole holder.
        """
        with self._mutex:
            entry = self._lock_table.get(rid)
            if entry is None:
                self._lock_table[rid] = {"holders": {txn_id: LOCK_EXCLUSIVE}, "exclusive_holder": txn_id}
                return True

            holders = entry["holders"]
            exclusive_holder = entry["exclusive_holder"]

            # Already exclusive — idempotent
            if exclusive_holder == txn_id:
                return True

            # Another txn holds exclusive — fail
            if exclusive_holder is not None:
                return False

            # Check other shared holders (lock upgrade)
            other_shared = [t for t in holders if t != txn_id]
            if other_shared:
                return False

            holders[txn_id] = LOCK_EXCLUSIVE
            entry["exclusive_holder"] = txn_id
            return True

    def release_all(self, txn_id):
        """Release all locks held by txn_id. Called on commit or abort."""
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
        """Clear all locks (for testing / database restart)."""
        with self._mutex:
            self._lock_table.clear()



_global_lock_manager = LockManager()


def get_lock_manager():
    return _global_lock_manager


def reset_lock_manager():
    """Reset global lock manager state (useful between test runs)."""
    _global_lock_manager.reset()
