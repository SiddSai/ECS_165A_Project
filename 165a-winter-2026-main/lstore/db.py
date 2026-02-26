import os
import struct
from lstore.table import Table, USER_COL_OFFSET
from lstore.page import Page, PAGE_SIZE_BYTES
from lstore.bufferpool import BufferPool, FrameMeta, make_page_key, parse_page_key


class Database():

    def __init__(self):
        self.tables = []
        self.path = None
        self.bufferpool = BufferPool()

    def open(self, path):
        """
        Open the database at the given path.
        If data exists on disk, load all tables back into memory.
        """

        # Store path and ensure directory exists
        self.path = path
        os.makedirs(path, exist_ok=True)

        # Register the write callback so bufferpool can flush pages during eviction
        self.bufferpool.set_write_callback(self._write_single_page)

        # Check if fresh db -> load nothing
        catalog_path = os.path.join(path, "catalog.bin")
        if not os.path.exists(catalog_path):
            return

        with open(catalog_path, "rb") as f:
            num_tables = struct.unpack("<i", f.read(4))[0]

            # Table loading
            for _ in range(num_tables):

                # Read table metadata
                name_len = struct.unpack("<i", f.read(4))[0]
                name = f.read(name_len).decode("utf-8")
                num_columns = struct.unpack("<i", f.read(4))[0]
                key = struct.unpack("<i", f.read(4))[0]

                try:
                    # Reconstruct table object
                    table = Table(name, num_columns, key)

                    # Reference to request pages from buffer pool when needed
                    table.bufferpool = self.bufferpool
                    table.db_path = self.path

                    # Load table data from disk (page ranges, page directory, meta)
                    table.deserialize(self.path)

                    # Pre-populate buffer pool with loaded pages to avoid cold-start cache misses
                    self._populate_bufferpool(table)

                    # Rebuild primary key index by scanning base page directory entries
                    self._rebuild_primary_index(table)

                    self.tables.append(table)
                except Exception:
                    # Skip tables that cannot be loaded (e.g. schema mismatch from old runs)
                    pass

    def close(self):
        """
        Write all table data to disk so it can be recovered on the next open().
        """
        # Check if open was called to begin with
        if self.path is None:
            return

        # Ensure directory exists
        os.makedirs(self.path, exist_ok=True)

        # Flush all dirty pages in buffer pool to disk before writing table and catalog files
        if self.bufferpool is not None:
            self.bufferpool.flush_all(self._write_single_page)

        # Write each table's page ranges and page directory to disk
        for table in self.tables:
            table.serialize(self.path)

        # Write catalog file: table names, num_columns, key index
        catalog_path = os.path.join(self.path, "catalog.bin")
        with open(catalog_path, "wb") as f:
            f.write(struct.pack("<i", len(self.tables)))

            for table in self.tables:
                # Write table metadata
                name_bytes = table.name.encode("utf-8")
                f.write(struct.pack("<i", len(name_bytes)))
                f.write(name_bytes)
                f.write(struct.pack("<i", table.num_columns))
                f.write(struct.pack("<i", table.key))

    # ------------------------------------------------------------------
    # Single page disk I/O (used by bufferpool as write callback)
    # ------------------------------------------------------------------

    def _write_single_page(self, page_key, page_obj):
        """
        Write one page to its correct position on disk.
        Called by the bufferpool on dirty page eviction or flush_all().

        page_key format: (table_name, range_id, is_tail, page_id, col_index)
        Each page is stored as an individual .bin file under:
            {db_path}/{table_name}/ranges/range_{range_id}/{prefix}_{page_id}_col_{col_index}.bin
        """
        if self.path is None:
            return

        table_name, range_id, is_tail, page_id, col_index = parse_page_key(page_key)
        prefix = "tail" if is_tail else "base"

        range_dir = os.path.join(self.path, table_name, "ranges", f"range_{range_id}")
        os.makedirs(range_dir, exist_ok=True)

        file_path = os.path.join(range_dir, f"{prefix}_{page_id}_col_{col_index}.bin")
        with open(file_path, "wb") as f:
            f.write(struct.pack("<i", page_obj.num_records))
            f.write(page_obj.to_bytes())

    # ------------------------------------------------------------------
    # Bufferpool warm-up
    # ------------------------------------------------------------------

    def _populate_bufferpool(self, table):
        """
        After loading page ranges from disk via table.deserialize(), insert all
        pages into the bufferpool so they are pre-cached and served on first access
        without triggering an extra disk read (cold-start miss avoidance).

        Pin count is set to 0: cached but not held by any active operation.
        """
        num_cols = table._num_cols_per_bundle()

        for range_id, page_range in enumerate(table.page_ranges):

            for page_id, bundle in enumerate(page_range.base_pages):
                for col_index in range(num_cols):
                    page_key = make_page_key(table.name, range_id, False, page_id, col_index)
                    if page_key not in self.bufferpool.frames:
                        if len(self.bufferpool.frames) >= self.bufferpool.pool_size:
                            self.bufferpool._evict()
                        frame = FrameMeta(page_key, bundle[col_index])
                        frame.pin_count = 0
                        frame.is_dirty = False
                        self.bufferpool.frames[page_key] = frame
                        self.bufferpool.lru_order[page_key] = True

            for page_id, bundle in enumerate(page_range.tail_pages):
                for col_index in range(num_cols):
                    page_key = make_page_key(table.name, range_id, True, page_id, col_index)
                    if page_key not in self.bufferpool.frames:
                        if len(self.bufferpool.frames) >= self.bufferpool.pool_size:
                            self.bufferpool._evict()
                        frame = FrameMeta(page_key, bundle[col_index])
                        frame.pin_count = 0
                        frame.is_dirty = False
                        self.bufferpool.frames[page_key] = frame
                        self.bufferpool.lru_order[page_key] = True

    # ------------------------------------------------------------------
    # Index reconstruction
    # ------------------------------------------------------------------

    def _rebuild_primary_index(self, table):
        """
        After loading from disk, rebuild the primary key index by scanning
        all non-tail entries in the page directory and calling table.read()
        to follow any indirection and get the current value.

        Only base RIDs are indexed; tail RIDs are an internal implementation detail.
        """
        for rid, (range_id, is_tail, page_id, offset) in table.page_directory.items():
            if is_tail:
                continue  # Only index base records
            record = table.read(rid)
            if record is None:
                continue  # Skip deleted records
            key_value = record.columns[table.key]
            table.index.insert(key_value, rid)

    # ------------------------------------------------------------------
    # Table management
    # ------------------------------------------------------------------

    def create_table(self, name, num_columns, key_index):
        """
        Creates a new table.
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key_index: int       #Index of table key in columns

        If a table with this name already exists (e.g. loaded by open() from a
        previous run), it is dropped and replaced with a fresh empty table.
        """
        # Drop any existing table with this name so create_table always starts fresh
        existing = self.get_table(name)
        if existing is not None:
            self.tables.remove(existing)

        # Evict all bufferpool pages belonging to this table so the new table
        # gets fresh pages instead of stale ones from a previous run
        stale_keys = [k for k in self.bufferpool.frames if k[0] == name]
        for k in stale_keys:
            del self.bufferpool.frames[k]
            if k in self.bufferpool.lru_order:
                del self.bufferpool.lru_order[k]

        table = Table(name, num_columns, key_index)

        # Give new table access to bufferpool and db path
        table.bufferpool = self.bufferpool
        table.db_path = self.path

        self.tables.append(table)
        return table

    def drop_table(self, name):
        """
        Deletes the specified table.
        """
        if table := self.get_table(name):
            self.tables.remove(table)
        else:
            raise Exception(f"Warning: Table named {name} does not exist")

    def get_table(self, name):
        """
        Returns table with the passed name.
        """
        for table in self.tables:
            if table.name == name:
                return table
        return None