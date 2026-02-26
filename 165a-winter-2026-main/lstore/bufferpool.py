import os
import struct
from collections import OrderedDict
from lstore.page import PAGE_SIZE_BYTES, INT64_BYTES


# Default max number of page frames in the bufferpool
DEFAULT_POOL_SIZE = 32


class FrameMeta:
    def __init__(self, page_key, page_obj):
        self.page_key = page_key
        self.page_obj = page_obj
        self.is_dirty = False
        self.pin_count = 0


class BufferPool:
    def __init__(self, pool_size=DEFAULT_POOL_SIZE):
        self.pool_size = pool_size
        self.frames = {}
        self.lru_order = OrderedDict()
        self._write_fn = None

    def get_page(self, page_key, load_fn):
        if page_key in self.frames:
            # Cache hit — move to end of LRU (most recently used)
            self.lru_order.move_to_end(page_key)
            frame = self.frames[page_key]
            frame.pin_count += 1
            return frame.page_obj

        # Cache miss — need to load from disk
        page_obj = load_fn(page_key)

        # If pool is full, evict a page to make room
        if len(self.frames) >= self.pool_size:
            self._evict()

        # Insert new frame
        frame = FrameMeta(page_key, page_obj)
        frame.pin_count = 1
        self.frames[page_key] = frame
        self.lru_order[page_key] = True
        self.lru_order.move_to_end(page_key)

        return page_obj

    def unpin(self, page_key, is_dirty=False):
        if page_key not in self.frames:
            return

        frame = self.frames[page_key]

        if frame.pin_count > 0:
            frame.pin_count -= 1

        if is_dirty:
            frame.is_dirty = True

    def mark_dirty(self, page_key):
        """
        Explicitly mark a page as dirty without unpinning.
        """
        if page_key in self.frames:
            self.frames[page_key].is_dirty = True

    def flush_all(self, write_fn):
        """
        Write all dirty pages to disk. Called when the database is closed.
        """
        for page_key, frame in list(self.frames.items()):
            if frame.is_dirty:
                write_fn(page_key, frame.page_obj)
                frame.is_dirty = False

    def flush_page(self, page_key, write_fn):
        """
        Force flush a single page to disk immediately.
        """
        if page_key in self.frames:
            frame = self.frames[page_key]
            if frame.is_dirty:
                write_fn(page_key, frame.page_obj)
                frame.is_dirty = False

    def is_in_pool(self, page_key):
        """Check if a page is currently in the bufferpool."""
        return page_key in self.frames

    def get_pool_stats(self):
        """
        Return current pool statistics. Useful for debugging and performance analysis.
        """
        total = len(self.frames)
        dirty = sum(1 for f in self.frames.values() if f.is_dirty)
        pinned = sum(1 for f in self.frames.values() if f.pin_count > 0)
        return {
            "pool_size": self.pool_size,
            "frames_used": total,
            "dirty_pages": dirty,
            "pinned_pages": pinned,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evict(self):
        """
        Evict the least recently used page that has pin_count == 0.
        """
        for page_key in list(self.lru_order.keys()):
            frame = self.frames[page_key]
            if frame.pin_count == 0:
                # Write back if dirty
                if frame.is_dirty and self._write_fn is not None:
                    self._write_fn(page_key, frame.page_obj)
                    frame.is_dirty = False

                # Remove from pool
                del self.frames[page_key]
                del self.lru_order[page_key]
                return

        raise RuntimeError(
            "BufferPool: cannot evict — all pages are pinned. "
            "Consider increasing pool size or unpinning pages sooner."
        )

    def register_page(self, page_key, page_obj):
        """
        Register a newly created in-memory page into the bufferpool.
        Called when a new Page is created (insert/update) so it is tracked
        for dirty-marking and eviction from the start.
        If the pool is full, evict first to make room.
        """
        if page_key in self.frames:
            return  # Already registered

        if len(self.frames) >= self.pool_size:
            self._evict()

        frame = FrameMeta(page_key, page_obj)
        frame.pin_count = 0
        frame.is_dirty = False
        self.frames[page_key] = frame
        self.lru_order[page_key] = True
        self.lru_order.move_to_end(page_key)

    def set_write_callback(self, write_fn):
        """
        Register the disk-write callback used during eviction.
        """
        self._write_fn = write_fn


# ----------------------------------------------------------------------
# Page-key helpers
# page_key format: (table_name, range_id, is_tail, page_id, col_index)
# range_id is REQUIRED because page_id resets to 0 in each new page range.
# Without range_id, different ranges would share the same key and collide.
# ----------------------------------------------------------------------

def make_page_key(table_name, range_id, is_tail, page_id, col_index):
    """
    Build a canonical page key.
    (table_name, range_id, is_tail, page_id, col_index)
    """
    return (table_name, range_id, is_tail, page_id, col_index)


def parse_page_key(page_key):
    """
    Unpack a page key into its components.
    Returns: (table_name, range_id, is_tail, page_id, col_index)
    """
    table_name, range_id, is_tail, page_id, col_index = page_key
    return table_name, range_id, is_tail, page_id, col_index