import os
import struct
from lstore.table import Table
from lstore.page import Page, PAGE_SIZE_BYTES
from lstore.bufferpool import BufferPool, FrameMeta, make_page_key
from lstore.index import Index


# Set size for each page entry on disk (4 bytes for num_records + 4096 bytes for page data)
PAGE_ENTRY_SIZE = 4 + PAGE_SIZE_BYTES

class Database():

    def __init__(self): 
        self.tables = []
        self.path = None
        self.bufferpool = None

    def open(self, path):
        """
        Open the database at the given path.
        If data exists on disk, load all tables back into memory.
        """

        # Store path
        self.path = path
        os.makedirs(path, exist_ok=True)

        # Initialize buffer pool with write callback for page eviction
        self.bufferpool = BufferPool()
        self.bufferpool.set_write_callback(self._write_single_page)


        # Check if fresh db -> load nothing
        catalog_path = os.path.join(path, "catalog.bin")
        if not os.path.exists(catalog_path):
            return 

        with open(catalog_path, "rb") as f:
            num_tables = struct.unpack("<I", f.read(4))[0]

            # Table loading 
            for _ in range(num_tables):
                
                # Read table metadata
                name_len = struct.unpack("<I", f.read(4))[0]
                name = f.read(name_len).decode("utf-8")
                num_columns, key, next_rid, next_tail_rid, num_base_bundles, num_tail_bundles = struct.unpack("<IiIIII", f.read(24))

                # Reconstruct table  
                table = Table(name, num_columns, key)
                table.next_rid = next_rid
                table.next_tail_rid = next_tail_rid

                # Reference to request pages from buffer pool when needed
                table.bufferpool = self.bufferpool
                table.db_path = self.path

                # Store number of bundles on disk
                table.num_base_bundles = num_base_bundles
                table.num_tail_bundles = num_tail_bundles

                # Load base pages
                table.base_pages = self._load_page_bundles(
                    os.path.join(path, name, "base_pages.bin"),
                    num_columns
                )

                # Load tail pages
                table.tail_pages = self._load_page_bundles(
                    os.path.join(path, name, "tail_pages.bin"),
                    num_columns
                )

                # Pre-populate buffer pool with loaded pages
                self._populate_bufferpool(table)

                # Load page directory
                table.page_directory = self._load_page_directory(
                    os.path.join(path, name, "page_directory.bin")
                )

                # Rebuild primary key index by scanning base page directory entries
                self._rebuild_primary_index(table)

                self.tables.append(table)

    def close(self):
        """
        Write all table data to disk so it can be recovered on the next open().
        """
        # Check if open was called to begin with
        if self.path is None:
            return

        # Ensure directory exists
        os.makedirs(self.path, exist_ok=True)

        # Flush all dirty pages in buffer pool to disk before writing catalog and page files
        if self.bufferpool is not None:
            self.bufferpool.flush_all(self._write_single_page)

        # Write catalog file with metadata for all tables
        catalog_path = os.path.join(self.path, "catalog.bin")
        with open(catalog_path, "wb") as f:
            f.write(struct.pack("<I", len(self.tables)))

            for table in self.tables:

                # Write table metadata
                name_bytes = table.name.encode("utf-8")
                f.write(struct.pack("<I", len(name_bytes)))
                f.write(name_bytes)
                num_base_bundles = len(table.base_pages)
                num_tail_bundles = len(table.tail_pages)
                f.write(struct.pack("<IiIIII",
                    table.num_columns,
                    table.key,
                    table.next_rid,
                    table.next_tail_rid,
                    num_base_bundles,
                    num_tail_bundles,
                ))

                table_dir = os.path.join(self.path, table.name)
                os.makedirs(table_dir, exist_ok=True)

                # Write base pages
                self._save_page_bundles(
                    os.path.join(table_dir, "base_pages.bin"),
                    table.base_pages,
                    table.num_columns
                )

                # Write tail pages
                self._save_page_bundles(
                    os.path.join(table_dir, "tail_pages.bin"),
                    table.tail_pages,
                    table.num_columns
                )

                # Write page directory
                self._save_page_directory(
                    os.path.join(table_dir, "page_directory.bin"),
                    table.page_directory
                )

    # ------------------------------------------------------------------
    # Disk I/O helpers — bulk operations
    # ------------------------------------------------------------------

    def _save_page_bundles(self, filepath, bundles, num_columns):
        """
        Save a list of page bundles to a binary file.
        
        File format:
            [num_bundles: 4 bytes]
            For each bundle:
                For each of (num_columns + 4) pages:
                    [num_records: 4 bytes]
                    [page data: 4096 bytes]
        """
        with open(filepath, "wb") as f:
            f.write(struct.pack("<I", len(bundles)))
            for bundle in bundles:
                pages_per_bundle = num_columns + 4
                for i in range(pages_per_bundle):
                    page = bundle[i]
                    f.write(struct.pack("<I", page.num_records))
                    f.write(page.data)

    def _load_page_bundles(self, filepath, num_columns):
        """
        Load page bundles from a binary file.
        Returns a list of bundles (each bundle is a list of Page objects).
        """
        if not os.path.exists(filepath):
            return []

        bundles = []
        pages_per_bundle = num_columns + 4

        with open(filepath, "rb") as f:
            num_bundles = struct.unpack("<I", f.read(4))[0]
            for _ in range(num_bundles):
                bundle = []
                for _ in range(pages_per_bundle):
                    page = Page()
                    page.num_records = struct.unpack("<I", f.read(4))[0]
                    page.data = bytearray(f.read(PAGE_SIZE_BYTES))
                    bundle.append(page)
                bundles.append(bundle)

        return bundles

    # ------------------------------------------------------------------
    # Single page operations (used by bufferpool)
    # ------------------------------------------------------------------

    def _page_file_path(self, table_name, is_tail):
        """Return the file path for base or tail pages of a given table."""
        filename = "tail_pages.bin" if is_tail else "base_pages.bin"
        return os.path.join(self.path, table_name, filename)

    def _page_byte_offset(self, page_id, col_index, num_columns):
        """
        Calculate byte offset of a specific page within a bundle file.
        Skips the 4-byte num_bundles header, then jumps to the right page.
        Each page entry is PAGE_ENTRY_SIZE (4100) bytes on disk.
        """
        pages_per_bundle = num_columns + 4
        page_number = page_id * pages_per_bundle + col_index
        return 4 + page_number * PAGE_ENTRY_SIZE

    def _write_single_page(self, page_key, page_obj):
        """
        Write one page to its correct position on disk.
        Called by bufferpool on dirty page eviction or flush_all().
        page_key = (table_name, is_tail, page_id, col_index)
        """
        table_name, is_tail, page_id, col_index = page_key

        table = self.get_table(table_name)
        if table is None:
            return

        filepath = self._page_file_path(table_name, is_tail)
        byte_offset = self._page_byte_offset(page_id, col_index, table.num_columns)

        # Create file if it doesn't exist
        if not os.path.exists(filepath):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "wb") as f:
                pass

        # Open in r+b to overwrite at specific position without truncating
        with open(filepath, "r+b") as f:
            # Extend file if needed
            f.seek(0, 2)
            file_size = f.tell()
            if byte_offset + PAGE_ENTRY_SIZE > file_size:
                f.write(b'\x00' * (byte_offset + PAGE_ENTRY_SIZE - file_size))

            # Seek to page position and write
            f.seek(byte_offset)
            f.write(struct.pack("<I", page_obj.num_records))
            f.write(page_obj.data)

    def _load_single_page(self, page_key):
        """
        Load one page from disk. Called by bufferpool on cache miss.
        page_key = (table_name, is_tail, page_id, col_index)
        Returns a Page object.
        """
        table_name, is_tail, page_id, col_index = page_key

        table = self.get_table(table_name)
        if table is None:
            return Page()

        filepath = self._page_file_path(table_name, is_tail)
        byte_offset = self._page_byte_offset(page_id, col_index, table.num_columns)

        if not os.path.exists(filepath):
            return Page()

        with open(filepath, "rb") as f:
            f.seek(byte_offset)
            raw = f.read(PAGE_ENTRY_SIZE)
            if len(raw) < PAGE_ENTRY_SIZE:
                return Page()

            num_records = struct.unpack("<I", raw[:4])[0]
            page = Page()
            page.num_records = num_records
            page.data = bytearray(raw[4:])
            return page

    # ------------------------------------------------------------------
    # Bufferpool helpers
    # ------------------------------------------------------------------

    def _populate_bufferpool(self, table):
        """
        After bulk-loading pages from disk, insert them into the bufferpool
        so they're pre-cached. Pin count = 0 (cached but not in active use).
        """
        pages_per_bundle = table.num_columns + 4

        for page_id, bundle in enumerate(table.base_pages):
            for col_index in range(pages_per_bundle):
                page_key = make_page_key(table.name, False, page_id, col_index)
                if page_key not in self.bufferpool.frames:
                    # Evict if pool is full
                    if len(self.bufferpool.frames) >= self.bufferpool.pool_size:
                        self.bufferpool._evict()
                    frame = FrameMeta(page_key, bundle[col_index])
                    frame.pin_count = 0
                    frame.is_dirty = False
                    self.bufferpool.frames[page_key] = frame
                    self.bufferpool.lru_order[page_key] = True

        for page_id, bundle in enumerate(table.tail_pages):
            for col_index in range(pages_per_bundle):
                page_key = make_page_key(table.name, True, page_id, col_index)
                if page_key not in self.bufferpool.frames:
                    if len(self.bufferpool.frames) >= self.bufferpool.pool_size:
                        self.bufferpool._evict()
                    frame = FrameMeta(page_key, bundle[col_index])
                    frame.pin_count = 0
                    frame.is_dirty = False
                    self.bufferpool.frames[page_key] = frame
                    self.bufferpool.lru_order[page_key] = True

    # ------------------------------------------------------------------
    # Page directory I/O
    # ------------------------------------------------------------------

    def _save_page_directory(self, filepath, page_directory):
        """
        Save the page directory dict to a binary file.
        
        File format:
            [num_entries: 4 bytes]
            For each entry:
                [rid: 8 bytes (signed)]
                [range_id: 4 bytes (signed)]
                [is_tail: 1 byte]
                [page_id: 4 bytes (signed)]
                [offset: 4 bytes (signed)]
        """
        with open(filepath, "wb") as f:
            f.write(struct.pack("<I", len(page_directory)))
            for rid, (range_id, is_tail, page_id, offset) in page_directory.items():
                f.write(struct.pack("<q", rid))                  # rid (signed 64-bit)
                f.write(struct.pack("<i", range_id))             # range_id
                f.write(struct.pack("<B", 1 if is_tail else 0))  # is_tail
                f.write(struct.pack("<i", page_id))              # page_id
                f.write(struct.pack("<i", offset))               # offset

    def _load_page_directory(self, filepath):
        """
        Load the page directory from a binary file.
        Returns a dict: rid -> (range_id, is_tail, page_id, offset)
        """
        if not os.path.exists(filepath):
            return {}

        page_directory = {}
        with open(filepath, "rb") as f:
            num_entries = struct.unpack("<I", f.read(4))[0]
            for _ in range(num_entries):
                rid = struct.unpack("<q", f.read(8))[0]
                range_id = struct.unpack("<i", f.read(4))[0]
                is_tail = struct.unpack("<B", f.read(1))[0] == 1
                page_id = struct.unpack("<i", f.read(4))[0]
                offset = struct.unpack("<i", f.read(4))[0]
                page_directory[rid] = (range_id, is_tail, page_id, offset)

        return page_directory

    def _rebuild_primary_index(self, table):
        """
        After loading from disk, rebuild the primary key index
        by scanning all non-deleted base records in the page directory.
        """
        for rid, (range_id, is_tail, page_id, offset) in table.page_directory.items():
            if is_tail:
                continue  # only index base records
            bundle = table.base_pages[page_id]
            key_value = bundle[table.key + 4].read(offset)
            table.index.insert(key_value, rid)

    # ------------------------------------------------------------------
    # Table management
    # ------------------------------------------------------------------

    
    def create_table(self, name, num_columns, key_index):
        """
        # Creates a new table
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        """
        if table := self.get_table(name):
            raise Exception(f"Warning: Table named {name} already exists")
        table = Table(name, num_columns, key_index)

        # Give new table access to bufferpool and db path
        table.bufferpool = self.bufferpool
        table.db_path = self.path
        table.num_base_bundles = 0
        table.num_tail_bundles = 0

        self.tables.append(table)
        return table

    
   
    def drop_table(self, name):
        """
        # Deletes the specified table
        """
        if table := self.get_table(name):
            self.tables.remove(table)
        else:
            raise Exception(f"Warning: Table named {name} does not exist")

    
    def get_table(self, name):
        """
        # Returns table with the passed name
        """
        for table in self.tables:
            if table.name == name:
                return table
        return None
