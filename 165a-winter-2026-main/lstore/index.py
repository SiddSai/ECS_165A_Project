import threading
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table

        # Create indexes in key column
        self.indices[table.key] = {}
        self._lock = threading.RLock()

    """
    # returns the location of all records with the given value in column "column"
    """

    def locate(self, column, value):
        with self._lock:
            if column < 0 or column >= self.table.num_columns:
                return []

            if self.indices[column] is None:
                return []

        return list(self.indices[column].get(value, []))

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        with self._lock:
            if column < 0 or column >= self.table.num_columns:
                return []

            # Check if does not exist
            if self.indices[column] is None:
                return []

            if begin > end:
                begin, end = end, begin

            result = []
            index = self.indices[column]

            for value in range(begin, end + 1):
                if value in index:
                    result.extend(index[value])
            
            return list(result)


    """
    Insert key value and rid into index.
    Supports all currently active indices, not just the primary key.
    """
    def insert(self, key_value, rid):
        with self._lock:
            # Always update the primary key index
            if self.indices[self.table.key] is not None:
                if key_value not in self.indices[self.table.key]:
                    self.indices[self.table.key][key_value] = []
                if rid not in self.indices[self.table.key][key_value]:
                    self.indices[self.table.key][key_value].append(rid)

    def insert_secondary(self, col, value, rid):
        """
        Insert a value and rid into the index for a non-primary-key column.
        Called during insert and when a secondary index is created or updated.
        """
        with self._lock:
            if col < 0 or col >= self.table.num_columns:
                return

            if self.indices[col] is not None and col != self.table.key:
                if value not in self.indices[col]:
                    self.indices[col][value] = []
                if rid not in self.indices[col][value]:
                    self.indices[col][value].append(rid)

    """
    Delete key value and rid from index.
    Supports all currently active indices, not just the primary key.
    """
    def delete(self, key_value, rid):
        with self._lock:
            # Always update the primary key index
            if self.indices[self.table.key] is not None:
                if key_value in self.indices[self.table.key]:
                    if rid in self.indices[self.table.key][key_value]:
                        self.indices[self.table.key][key_value].remove(rid)
                    if len(self.indices[self.table.key][key_value]) == 0:
                        del self.indices[self.table.key][key_value]

    def delete_secondary(self, col, value, rid):
        """
        Remove a value and rid from the index for a non-primary-key column.
        Called during delete and update for all active secondary indices.
        """
        with self._lock:
            if col < 0 or col >= self.table.num_columns:
                return

            if self.indices[col] is not None and col != self.table.key:
                if value in self.indices[col]:
                    if rid in self.indices[col][value]:
                        self.indices[col][value].remove(rid)
                    if len(self.indices[col][value]) == 0:
                        del self.indices[col][value]


    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        with self._lock:
            if column_number < 0 or column_number >= self.table.num_columns:
                return

            if self.indices[column_number] is not None:
                return  # Index already exists

            self.indices[column_number] = {}

            # Scan all existing records and populate the new index
            for rid, (range_id, is_tail, page_id, offset) in list(self.table.page_directory.items()):
                if is_tail:
                    continue  # Only index base records

                # Read the latest value for this record by following indirection
                record = self.table.read(rid)
                if record is None:
                    continue

                value = record.columns[column_number]
                if value not in self.indices[column_number]:
                    self.indices[column_number][value] = []
                if rid not in self.indices[column_number][value]:
                    self.indices[column_number][value].append(rid)

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        self.indices[column_number] = None