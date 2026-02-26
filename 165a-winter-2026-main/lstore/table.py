import os
import struct
import threading
from lstore.index import Index
from lstore.page import Page, PAGE_SIZE_BYTES, INT64_BYTES
from lstore.bufferpool import make_page_key
from time import time

"""
The Table class provides the core of our relational storage functionality. All columns are 64-bit integers in this implementation. Users mainly interact with tables through queries. Tables provide a logical view of the actual physically stored data and mostly manage the storage and retrieval of data. Each table is responsible for managing its pages and requires an internal page directory that, given a RID, returns the actual physical location of the record. The table class should also manage the periodical merge of its corresponding page ranges.
"""

# ------------------------------------------------------------------
# Physical column layout constants (per bundle)
# ------------------------------------------------------------------
INDIRECTION_COLUMN = 0  # points to latest tail RID (or NULL_RID / deleted sentinel)
RID_COLUMN = 1  # the RID stored in this slot
TIMESTAMP_COLUMN = 2  # creation / update timestamp
SCHEMA_ENCODING_COLUMN = 3  # bitmask of which user columns have been updated
BASE_RID_COLUMN = 4  # tail records: RID of the base record this tail belongs to
# base records: its own RID (used by merge for O(1) lookup)
USER_COL_OFFSET = 5  # user-visible columns start at physical index 5

NULL_RID = -1
PAGES_PER_RANGE = 16


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class PageRange:
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.base_pages = []
        self.tail_pages = []
        # Tail-Page Sequence Number (TPS) per base page_id. Each entry is the
        # highest tail RID that has been merged into that base page.
        self.tps = []
        # Background merge writes the merged copy here; foreground operations
        # apply it to make the change visible.
        self.pending_merge = None

    def has_capacity(self):
        if len(self.base_pages) == 0:
            return True
        if len(self.base_pages) >= PAGES_PER_RANGE:
            return False
        return True


# need page ranges
class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns

        self.page_directory = {}
        self.index = Index(self)
        self.merge_threshold_pages = 10  # Merge threshold - set high to avoid corrupting historical queries

        self.next_rid = 0
        # Tail RIDs start at 2**32 to avoid colliding with base RIDs in page_directory.
        # Base RIDs grow upward from 0; tail RIDs grow upward from TAIL_RID_BASE.
        self.next_tail_rid = 2 ** 32
        self.page_ranges = []

        self.bufferpool = None  # Assigned by db.open() / db.create_table()
        self.db_path = None  # Set by db.open() so table can load pages from disk
        self.update_count = 0  # Counts updates; triggers merge when threshold is reached
        self.merge_in_progress = False  # Prevents multiple merge threads from running at once
        self.merge_lock = threading.Lock()  # Protects base_pages during background merge

    # Sentinel value written to INDIRECTION_COLUMN to mark a record as deleted
    deleted = -5

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_pending_merge(self, range_id: int):
        """Apply a prepared merge result for the given range in the foreground.

        Milestone 2 expects background merge to build a merged copy, while the
        foreground briefly blocks to update the page directory / visible pages.
        We model this by letting the merge thread write into PageRange.pending_merge
        and applying it opportunistically from normal foreground operations.
        """
        if range_id < 0 or range_id >= len(self.page_ranges):
            return
        page_range = self.page_ranges[range_id]
        pending = page_range.pending_merge
        if pending is None:
            return

        merged_base, merged_tps = pending
        with self.merge_lock:
            # Another foreground thread may have already applied it.
            if page_range.pending_merge is None:
                return
            page_range.base_pages = merged_base
            # Grow TPS list if needed
            if len(page_range.tps) < len(page_range.base_pages):
                page_range.tps.extend([NULL_RID] * (len(page_range.base_pages) - len(page_range.tps)))
            # Apply merged TPS for the pages covered by this merge
            for page_id, tps_val in enumerate(merged_tps):
                if page_id < len(page_range.tps):
                    page_range.tps[page_id] = max(page_range.tps[page_id], tps_val)
            page_range.pending_merge = None

            # Keep bufferpool frames consistent (if enabled)
            if self.bufferpool is not None:
                for page_id, bundle in enumerate(page_range.base_pages):
                    for col_idx, page in enumerate(bundle):
                        page_key = make_page_key(self.name, range_id, False, page_id, col_idx)
                        frame = self.bufferpool.frames.get(page_key)
                        if frame is not None:
                            frame.page_obj = page

    def _num_cols_per_bundle(self):
        """Total number of physical page columns per bundle (5 meta + user cols)."""
        return USER_COL_OFFSET + self.num_columns

    def _get_bundle_direct(self, range_id, is_tail, page_id):
        """Direct in-memory bundle access, bypassing the bufferpool."""
        page_range = self.page_ranges[range_id]
        if is_tail:
            return page_range.tail_pages[page_id]
        else:
            return page_range.base_pages[page_id]

    def _load_page_from_disk(self, page_key):
        table_name, range_id, is_tail, page_id, col_index = page_key
        prefix = "tail" if is_tail else "base"

        if self.db_path is not None:
            range_dir = os.path.join(self.db_path, self.name, "ranges", f"range_{range_id}")
            file_path = os.path.join(range_dir, f"{prefix}_{page_id}_col_{col_index}.bin")
            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    num_records = struct.unpack("<i", f.read(4))[0]
                    raw = f.read(PAGE_SIZE_BYTES)
                return Page.from_bytes(raw, num_records)

        # Fallback: return the in-memory object directly (e.g. before first flush)
        if range_id < len(self.page_ranges):
            page_range = self.page_ranges[range_id]
            pages = page_range.tail_pages if is_tail else page_range.base_pages
            if page_id < len(pages) and col_index < len(pages[page_id]):
                return pages[page_id][col_index]

        return Page()  # Should not reach here in normal use

    def _get_bundle(self, range_id, is_tail, page_id):

        # Foreground application of any completed background merge
        if not is_tail:
            self._apply_pending_merge(range_id)

        if self.bufferpool is None or self.db_path is None:
            return self._get_bundle_direct(range_id, is_tail, page_id)

        num_cols = self._num_cols_per_bundle()
        bundle = []
        for col_index in range(num_cols):
            page_key = make_page_key(self.name, range_id, is_tail, page_id, col_index)
            page_obj = self.bufferpool.get_page(page_key, self._load_page_from_disk)
            bundle.append(page_obj)
        return bundle

    def _unpin_bundle(self, range_id, is_tail, page_id, dirty=False):
        if self.bufferpool is None:
            return
        num_cols = self._num_cols_per_bundle()
        for col_index in range(num_cols):
            page_key = make_page_key(self.name, range_id, is_tail, page_id, col_index)
            self.bufferpool.unpin(page_key, is_dirty=dirty)

    def _register_bundle(self, range_id, is_tail, page_id, bundle):

        if self.bufferpool is None or self.db_path is None:
            return
        for col_index, page_obj in enumerate(bundle):
            page_key = make_page_key(self.name, range_id, is_tail, page_id, col_index)
            self.bufferpool.register_page(page_key, page_obj)

    # Core CRUD operations

    def _read_without_indirection(self, rid):
        """Read the record stored at the given RID without following any indirection chain."""
        if rid not in self.page_directory:
            return None

        range_id, is_tail, page_id, offset = self.page_directory[rid]
        bundle = self._get_bundle(range_id, is_tail, page_id)

        columns = []
        for col in range(self.num_columns):
            value = bundle[col + USER_COL_OFFSET].read(offset)
            columns.append(value)

        self._unpin_bundle(range_id, is_tail, page_id, dirty=False)
        return Record(rid, columns[self.key], columns)

    def _read_at_rid(self, rid):
        """Alias for _read_without_indirection (kept for compatibility)."""
        return self._read_without_indirection(rid)

    def insert(self, record):
        # Check for duplicate primary key BEFORE writing anything to pages
        if self.index.locate(self.key, record.columns[self.key]):
            return False

        # Generate a new RID
        rid = self.next_rid
        self.next_rid += 1

        # Create a new page range if none exist or the current one is full
        if len(self.page_ranges) == 0 or not self.page_ranges[-1].has_capacity():
            self.page_ranges.append(PageRange(self.num_columns))

        current_range = self.page_ranges[-1]
        range_id = len(self.page_ranges) - 1

        # Create a new base page bundle if none exist or the last one is full
        num_cols = self._num_cols_per_bundle()
        if (len(current_range.base_pages) == 0
                or not current_range.base_pages[-1][RID_COLUMN].has_capacity()):
            bundle = [Page() for _ in range(num_cols)]
            current_range.base_pages.append(bundle)
            if len(current_range.tps) < len(current_range.base_pages):
                current_range.tps.append(NULL_RID)
            new_page_id = len(current_range.base_pages) - 1
            self._register_bundle(range_id, False, new_page_id, bundle)

        page_id = len(current_range.base_pages) - 1
        current = self._get_bundle(range_id, False, page_id)

        current[INDIRECTION_COLUMN].write(NULL_RID)  # null for base page
        offset = current[RID_COLUMN].write(rid)
        current[TIMESTAMP_COLUMN].write(int(time()))
        current[SCHEMA_ENCODING_COLUMN].write(0)  # no columns updated yet
        current[BASE_RID_COLUMN].write(rid)  # base record stores its own RID
        for col in range(self.num_columns):
            current[col + USER_COL_OFFSET].write(record.columns[col])

        # Mark pages dirty and unpin
        self._unpin_bundle(range_id, False, page_id, dirty=True)

        # Update page directory
        self.page_directory[rid] = (range_id, False, page_id, offset)

        self.index.insert(record.columns[self.key], rid)

        # Update any active secondary indexes
        for col in range(self.num_columns):
            if col != self.key:
                self.index.insert_secondary(col, record.columns[col], rid)

        return True

    # future: implement read_latest()
    def read(self, rid):
        # Retrieve record with the given RID by looking up the page directory.
        # Uses iteration instead of recursion to handle long tail chains efficiently.
        if rid not in self.page_directory:
            return None

        current_rid = rid
        while True:
            if current_rid not in self.page_directory:
                return None

            range_id, is_tail, page_id, offset = self.page_directory[current_rid]
            bundle = self._get_bundle(range_id, is_tail, page_id)

            indirection = bundle[INDIRECTION_COLUMN].read(offset)
            columns = [bundle[col + USER_COL_OFFSET].read(offset) for col in range(self.num_columns)]
            key_value = columns[self.key]

            self._unpin_bundle(range_id, is_tail, page_id, dirty=False)

            # Deleted record
            if indirection == self.deleted:
                return None

            # Always follow indirection from base to latest tail.
            # (TPS optimization disabled to preserve historical query correctness)
            if not is_tail and indirection != NULL_RID:
                current_rid = indirection
                continue

            return Record(current_rid, key_value, columns)

    def update(self, rid, columns):
        # rid is the BASE rid
        if len(columns) != self.num_columns:
            return False

        # Primary key updates are not allowed
        if columns[self.key] is not None:
            return False

        # Make sure rid exists
        if rid not in self.page_directory:
            return False

        (range_id, is_tail, page_id, offset) = self.page_directory[rid]

        # Updates should target base records, not tail records
        if is_tail:
            return False

        base_rid = rid
        base_bundle = self._get_bundle(range_id, False, page_id)

        # Read current (latest) values to build a cumulative tail record
        prev_tail_rid = base_bundle[INDIRECTION_COLUMN].read(offset)
        if prev_tail_rid == NULL_RID:
            # No prior tail — read from base bundle directly
            current_values = [
                base_bundle[col + USER_COL_OFFSET].read(offset)
                for col in range(self.num_columns)
            ]
        else:
            if prev_tail_rid not in self.page_directory:
                self._unpin_bundle(range_id, False, page_id, dirty=False)
                return False
            t_range, t_is_tail, t_page_id, t_offset = self.page_directory[prev_tail_rid]
            tail_bundle_prev = self._get_bundle(t_range, t_is_tail, t_page_id)
            current_values = [
                tail_bundle_prev[col + USER_COL_OFFSET].read(t_offset)
                for col in range(self.num_columns)
            ]
            self._unpin_bundle(t_range, t_is_tail, t_page_id, dirty=False)

        # Build update bitmask
        update_mask = 0
        for i in range(self.num_columns):
            if columns[i] is not None:
                update_mask |= (1 << i)
        if update_mask == 0:
            self._unpin_bundle(range_id, False, page_id, dirty=False)
            return False

        # Compute new cumulative column values
        new_values = current_values.copy()
        for i in range(self.num_columns):
            if columns[i] is not None:
                new_values[i] = columns[i]

        new_tail_rid = self.next_tail_rid
        self.next_tail_rid += 1

        # Create a new tail page bundle if needed
        num_cols = self._num_cols_per_bundle()
        tails = self.page_ranges[range_id].tail_pages
        if (len(tails) == 0) or (not tails[-1][RID_COLUMN].has_capacity()):
            bundle = [Page() for _ in range(num_cols)]
            tails.append(bundle)
            new_tail_page_id = len(tails) - 1
            self._register_bundle(range_id, True, new_tail_page_id, bundle)

        tail_page_id = len(tails) - 1
        tail_bundle = self._get_bundle(range_id, True, tail_page_id)

        # Write new tail record (cumulative — stores all column values)
        tail_offset = tail_bundle[RID_COLUMN].write(new_tail_rid)
        tail_bundle[INDIRECTION_COLUMN].write(prev_tail_rid)  # chain to previous tail
        tail_bundle[TIMESTAMP_COLUMN].write(int(time()))
        tail_bundle[SCHEMA_ENCODING_COLUMN].write(update_mask)
        tail_bundle[BASE_RID_COLUMN].write(base_rid)  # direct link to base record
        for col in range(self.num_columns):
            tail_bundle[col + USER_COL_OFFSET].write(new_values[col])

        self._unpin_bundle(range_id, True, tail_page_id, dirty=True)

        # Register new tail in page directory
        self.page_directory[new_tail_rid] = (range_id, True, tail_page_id, tail_offset)

        # Update base record metadata: point indirection to new tail, merge schema bits
        base_bundle[INDIRECTION_COLUMN].update(offset, new_tail_rid)
        old_schema = base_bundle[SCHEMA_ENCODING_COLUMN].read(offset)
        base_bundle[SCHEMA_ENCODING_COLUMN].update(offset, old_schema | update_mask)
        self._unpin_bundle(range_id, False, page_id, dirty=True)

        # Maintain secondary indexes: remove stale entries, add updated ones
        for col in range(self.num_columns):
            if col == self.key:
                continue
            if self.index.indices[col] is None:
                continue
            old_val = current_values[col]
            new_val = new_values[col]
            if old_val != new_val:
                self.index.delete_secondary(col, old_val, base_rid)
                self.index.insert_secondary(col, new_val, base_rid)

        # Trigger background merge after enough updates have accumulated
        self.update_count += 1
        if self.update_count >= self.merge_threshold_pages and not self.merge_in_progress:
            self.update_count = 0
            self.merge_in_progress = True
            t = threading.Thread(target=self._Table__merge)
            t.daemon = True
            t.start()

        return True

    def delete(self, rid):
        # Mark the record with the given RID as deleted
        if rid not in self.page_directory:
            return False

        range_id, is_tail, page_id, offset = self.page_directory[rid]
        bundle = self._get_bundle(range_id, is_tail, page_id)

        # Read key value from the base page for primary index cleanup
        key_value = bundle[self.key + USER_COL_OFFSET].read(offset)

        # For secondary index cleanup, we need the *latest* values (may be in a tail record)
        indirection = bundle[INDIRECTION_COLUMN].read(offset)
        if (not is_tail and indirection != NULL_RID and indirection != self.deleted
                and indirection in self.page_directory):
            # Follow indirection to get latest values from the tail record
            t_range, t_is_tail, t_page_id, t_offset = self.page_directory[indirection]
            tail_bundle = self._get_bundle(t_range, t_is_tail, t_page_id)
            current_values = [
                tail_bundle[col + USER_COL_OFFSET].read(t_offset)
                for col in range(self.num_columns)
            ]
            self._unpin_bundle(t_range, t_is_tail, t_page_id, dirty=False)
        else:
            current_values = [
                bundle[col + USER_COL_OFFSET].read(offset)
                for col in range(self.num_columns)
            ]

        # Write deletion sentinel to INDIRECTION_COLUMN
        bundle[INDIRECTION_COLUMN].update(offset, self.deleted)
        self._unpin_bundle(range_id, is_tail, page_id, dirty=True)

        # Remove from primary key index
        self.index.delete(key_value, rid)

        # Remove from all active secondary indexes
        for col in range(self.num_columns):
            if col == self.key:
                continue
            if self.index.indices[col] is not None:
                self.index.delete_secondary(col, current_values[col], rid)

        # Keep the page_directory entry so deleted records survive serialization/deserialization.
        # Callers detect deletion via the INDIRECTION_COLUMN sentinel (self.deleted).

        return True

    # ------------------------------------------------------------------
    # Persistence: write to disk
    # ------------------------------------------------------------------

    def serialize(self, path):
        table_dir = os.path.join(path, self.name)
        os.makedirs(table_dir, exist_ok=True)

        # Write meta: next_rid, next_tail_rid, num_columns, key, number of page ranges
        meta_path = os.path.join(table_dir, "meta.bin")
        with open(meta_path, "wb") as f:
            f.write(struct.pack("<qqiii",
                                self.next_rid,
                                self.next_tail_rid,
                                self.num_columns,
                                self.key,
                                len(self.page_ranges)
                                ))

        # Write page_directory: each entry is rid(q) range_id(i) is_tail(i) page_id(i) offset(i)
        pd_path = os.path.join(table_dir, "page_directory.bin")
        with open(pd_path, "wb") as f:
            f.write(struct.pack("<q", len(self.page_directory)))
            for rid, (range_id, is_tail, page_id, offset) in self.page_directory.items():
                f.write(struct.pack("<qiiii",
                                    rid,
                                    range_id,
                                    1 if is_tail else 0,
                                    page_id,
                                    offset
                                    ))

        # Write page data for each page range
        ranges_dir = os.path.join(table_dir, "ranges")
        os.makedirs(ranges_dir, exist_ok=True)

        for i, page_range in enumerate(self.page_ranges):
            range_dir = os.path.join(ranges_dir, f"range_{i}")
            os.makedirs(range_dir, exist_ok=True)

            # Write range meta: number of base and tail bundles
            range_meta_path = os.path.join(range_dir, "range_meta.bin")
            with open(range_meta_path, "wb") as f:
                f.write(struct.pack("<ii",
                                    len(page_range.base_pages),
                                    len(page_range.tail_pages)
                                    ))

        # Write base pages: one file per column per bundle
        for j, bundle in enumerate(page_range.base_pages):
            for c, page in enumerate(bundle):
                file_path = os.path.join(range_dir, f"base_{j}_col_{c}.bin")
                # Prefer the bufferpool's copy if present (it may be newer than the in-memory ref)
                page_key = make_page_key(self.name, i, False, j, c)
                if self.bufferpool is not None and self.bufferpool.is_in_pool(page_key):
                    page = self.bufferpool.frames[page_key].page_obj
                with open(file_path, "wb") as f:
                    f.write(struct.pack("<i", page.num_records))
                    f.write(page.to_bytes())

        # Write tail pages: one file per column per bundle
        for j, bundle in enumerate(page_range.tail_pages):
            for c, page in enumerate(bundle):
                file_path = os.path.join(range_dir, f"tail_{j}_col_{c}.bin")
                # Prefer the bufferpool's copy if present (it may be newer than the in-memory ref)
                page_key = make_page_key(self.name, i, True, j, c)
                if self.bufferpool is not None and self.bufferpool.is_in_pool(page_key):
                    page = self.bufferpool.frames[page_key].page_obj
                with open(file_path, "wb") as f:
                    f.write(struct.pack("<i", page.num_records))
                    f.write(page.to_bytes())

    # Persistence: read from disk

    def deserialize(self, path):
        table_dir = os.path.join(path, self.name)

        # Read meta
        meta_path = os.path.join(table_dir, "meta.bin")
        with open(meta_path, "rb") as f:
            self.next_rid, self.next_tail_rid, self.num_columns, self.key, num_ranges = struct.unpack(
                "<qqiii", f.read(struct.calcsize("<qqiii"))
            )

        # Read page_directory
        pd_path = os.path.join(table_dir, "page_directory.bin")
        with open(pd_path, "rb") as f:
            count = struct.unpack("<q", f.read(8))[0]
            entry_size = struct.calcsize("<qiiii")
            for _ in range(count):
                rid, range_id, is_tail_int, page_id, offset = struct.unpack(
                    "<qiiii", f.read(entry_size)
                )
                self.page_directory[rid] = (range_id, bool(is_tail_int), page_id, offset)

        # Read each page range from disk
        ranges_dir = os.path.join(table_dir, "ranges")
        self.page_ranges = []
        num_cols_per_bundle = self._num_cols_per_bundle()

        for i in range(num_ranges):
            range_dir = os.path.join(ranges_dir, f"range_{i}")
            page_range = PageRange(self.num_columns)

            # Read range meta to get bundle counts
            range_meta_path = os.path.join(range_dir, "range_meta.bin")
            with open(range_meta_path, "rb") as f:
                num_base, num_tail = struct.unpack("<ii", f.read(8))

            # Read base page bundles
            for j in range(num_base):
                bundle = []
                for c in range(num_cols_per_bundle):
                    file_path = os.path.join(range_dir, f"base_{j}_col_{c}.bin")
                    with open(file_path, "rb") as f:
                        num_records = struct.unpack("<i", f.read(4))[0]
                        raw = f.read(PAGE_SIZE_BYTES)
                    bundle.append(Page.from_bytes(raw, num_records))
                page_range.base_pages.append(bundle)
                if len(page_range.tps) < len(page_range.base_pages):
                    page_range.tps.append(NULL_RID)

            # Read tail page bundles
            for j in range(num_tail):
                bundle = []
                for c in range(num_cols_per_bundle):
                    file_path = os.path.join(range_dir, f"tail_{j}_col_{c}.bin")
                    with open(file_path, "rb") as f:
                        num_records = struct.unpack("<i", f.read(4))[0]
                        raw = f.read(PAGE_SIZE_BYTES)
                    bundle.append(Page.from_bytes(raw, num_records))
                page_range.tail_pages.append(bundle)

            self.page_ranges.append(page_range)

    # ------------------------------------------------------------------
    # Background merge thread
    # ------------------------------------------------------------------

    def __merge(self):
        import copy
        # Background merge: build merged base-page copies and TPS updates, but do not
        # make them visible here. Foreground operations apply them via _apply_pending_merge().

        for range_id, page_range in enumerate(self.page_ranges):
            with self.merge_lock:
                if not page_range.tail_pages:
                    continue
                snapshot_tail_count = len(page_range.tail_pages)
                tail_snapshot = [page_range.tail_pages[i] for i in range(snapshot_tail_count)]
                tail_snapshot_sizes = [bundle[RID_COLUMN].num_records for bundle in tail_snapshot]
                merged_base = copy.deepcopy(page_range.base_pages)
                # TPS we will produce for each base page_id covered by this merge snapshot
                merged_tps = [page_range.tps[i] if i < len(page_range.tps) else NULL_RID for i in
                              range(len(merged_base))]

            # Apply tail records newest-first so only the latest update is applied per base record
            already_merged = set()
            for bundle_idx in reversed(range(snapshot_tail_count)):
                tail_bundle = tail_snapshot[bundle_idx]
                num_tail_records = tail_snapshot_sizes[bundle_idx]
                for slot in reversed(range(num_tail_records)):
                    base_rid = tail_bundle[BASE_RID_COLUMN].read(slot)
                    if base_rid == NULL_RID or base_rid == self.deleted:
                        continue
                    if base_rid in already_merged or base_rid not in self.page_directory:
                        continue
                    b_range_id, b_is_tail, b_page_id, b_offset = self.page_directory[base_rid]
                    if b_is_tail or b_range_id != range_id:
                        continue
                    tail_rid = tail_bundle[RID_COLUMN].read(slot)
                    # user column copy removed to preserve historical query correctness
                    merged_tps[b_page_id] = max(merged_tps[b_page_id], tail_rid)
                    already_merged.add(base_rid)

            # Reconcile tail records that arrived during merge (new bundles and growth of last bundle)
            with self.merge_lock:
                current_tail_count = len(page_range.tail_pages)

                # New bundles appended during merge
                for bundle_idx in range(snapshot_tail_count, current_tail_count):
                    tail_bundle = page_range.tail_pages[bundle_idx]
                    for slot in range(tail_bundle[RID_COLUMN].num_records):
                        base_rid = tail_bundle[BASE_RID_COLUMN].read(slot)
                        if base_rid == NULL_RID or base_rid == self.deleted:
                            continue
                        if base_rid not in self.page_directory:
                            continue
                        b_range_id, b_is_tail, b_page_id, b_offset = self.page_directory[base_rid]
                        if b_is_tail or b_range_id != range_id:
                            continue
                        tail_rid = tail_bundle[RID_COLUMN].read(slot)
                        # user column copy removed to preserve historical query correctness
                        merged_tps[b_page_id] = max(merged_tps[b_page_id], tail_rid)

                # Records appended to the last snapshot bundle during merge
                if snapshot_tail_count > 0:
                    last_bundle = page_range.tail_pages[snapshot_tail_count - 1]
                    old_size = tail_snapshot_sizes[snapshot_tail_count - 1]
                    new_size = last_bundle[RID_COLUMN].num_records
                    for slot in range(old_size, new_size):
                        base_rid = last_bundle[BASE_RID_COLUMN].read(slot)
                        if base_rid == NULL_RID or base_rid == self.deleted:
                            continue
                        if base_rid not in self.page_directory:
                            continue
                        b_range_id, b_is_tail, b_page_id, b_offset = self.page_directory[base_rid]
                        if b_is_tail or b_range_id != range_id:
                            continue
                        tail_rid = last_bundle[RID_COLUMN].read(slot)
                        # user column copy removed to preserve historical query correctness
                        merged_tps[b_page_id] = max(merged_tps[b_page_id], tail_rid)

                # Publish merge result for foreground to apply
                page_range.pending_merge = (merged_base, merged_tps)

        self.merge_in_progress = False