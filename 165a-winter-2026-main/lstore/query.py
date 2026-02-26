from lstore.table import Table, Record, USER_COL_OFFSET, RID_COLUMN, INDIRECTION_COLUMN, BASE_RID_COLUMN, NULL_RID
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            rid = rids[0]
            return bool(self.table.delete(rid))
        except Exception:
            return False

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        try:
            if len(columns) != self.table.num_columns:
                return False
            if any(v is None for v in columns):
                return False

            record = Record(None, columns[self.table.key], list(columns))

            ok = self.table.insert(record)
            return bool(ok)

        except Exception:
            return False

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            rids = self.table.index.locate(search_key_index, search_key)

            if not rids and self.table.index.indices[search_key_index] is None:
                rids = self._full_scan(search_key, search_key_index)

            results = []
            for rid in rids:
                rec_obj = self.table.read(rid)
                if rec_obj is None:
                    continue

                full_cols = rec_obj.columns

                projected = [None] * self.table.num_columns
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        projected[i] = full_cols[i]

                key_val = full_cols[self.table.key]
                results.append(Record(rid, key_val, projected))

            return results
        except Exception:
            return False

    def _full_scan(self, search_key, search_key_index):
        rids = []
        for rid, (range_id, is_tail, page_id, offset) in self.table.page_directory.items():
            if is_tail:
                continue
            rec = self.table.read(rid)
            if rec is None:
                continue
            if rec.columns[search_key_index] == search_key:
                rids.append(rid)
        return rids

    def _full_scan_base_rids(self, search_key, search_key_index):
        """
        Full scan returning base RIDs whose *latest* value matches search_key.
        Used by select_version when no index exists for the search column.
        """
        rids = []
        for rid, (range_id, is_tail, page_id, offset) in self.table.page_directory.items():
            if is_tail:
                continue
            rec = self.table.read(rid)   # follows indirection to latest version
            if rec is None:
                continue
            if rec.columns[search_key_index] == search_key:
                rids.append(rid)
        return rids

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            if relative_version == 0:
                return self.select(search_key, search_key_index, projected_columns_index)

            rids = self.table.index.locate(search_key_index, search_key)

            # Fall back to full scan if no index exists for this column (same as select())
            if not rids and self.table.index.indices[search_key_index] is None:
                rids = self._full_scan_base_rids(search_key, search_key_index)

            if not rids:
                return False

            results = []
            for base_rid in rids:
                if base_rid not in self.table.page_directory:
                    continue

                range_id, is_tail, page_id, offset = self.table.page_directory[base_rid]
                if is_tail:
                    continue

                # Read indirection from base bundle to get the latest tail RID
                base_bundle = self.table._get_bundle(range_id, False, page_id)
                latest_tail_rid = base_bundle[INDIRECTION_COLUMN].read(offset)
                self.table._unpin_bundle(range_id, False, page_id, dirty=False)

                # Start from latest tail and step backwards.
                # relative_version=-1 → step back once from latest_tail → reaches base (1 update case)
                # relative_version=-2 → step back twice → also base if only 1 update
                current_rid = latest_tail_rid
                steps = abs(relative_version)

                # If the record has never been updated, all versions resolve to base
                if current_rid == NULL_RID:
                    current_rid = base_rid

                for _ in range(steps):
                    if current_rid == NULL_RID:
                        # No more history — use base record
                        current_rid = base_rid
                        break
                    if current_rid == self.table.deleted:
                        break
                    if current_rid not in self.table.page_directory:
                        current_rid = base_rid
                        break

                    t_range, t_is_tail, t_page_id, t_offset = self.table.page_directory[current_rid]

                    if not t_is_tail:
                        # Already at base
                        break

                    # Step one version back by reading the tail's INDIRECTION pointer
                    tail_bundle = self.table._get_bundle(t_range, True, t_page_id)
                    next_rid = tail_bundle[INDIRECTION_COLUMN].read(t_offset)
                    self.table._unpin_bundle(t_range, True, t_page_id, dirty=False)

                    if next_rid == NULL_RID:
                        # Next pointer leads to base
                        current_rid = base_rid
                        break
                    else:
                        current_rid = next_rid

                # Read the record at the resolved version without following indirection
                rec_obj = self.table._read_without_indirection(current_rid)
                if rec_obj is None:
                    continue

                projected = [None] * self.table.num_columns
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        projected[i] = rec_obj.columns[i]

                results.append(Record(current_rid, rec_obj.key, projected))

            return results if results else False
        except Exception:
            return False

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        try:
            if len(columns) != self.table.num_columns:
                return False

            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            base_rid = rids[0]
            return bool(self.table.update(base_rid, list(columns)))

        except Exception:
            return False

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False
            total = 0
            # Physical column index for the target user column
            col = aggregate_column_index + USER_COL_OFFSET
            for rid in rids:
                if rid not in self.table.page_directory:
                    continue
                range_id, is_tail, page_id, offset = self.table.page_directory[rid]
                # Follow indirection to get latest value
                if not is_tail:
                    bundle = self.table._get_bundle(range_id, False, page_id)
                    indirection = bundle[INDIRECTION_COLUMN].read(offset)
                    if (indirection != NULL_RID and indirection != self.table.deleted
                            and indirection in self.table.page_directory):
                        self.table._unpin_bundle(range_id, False, page_id, dirty=False)
                        r2, t2, p2, o2 = self.table.page_directory[indirection]
                        tail_bundle = self.table._get_bundle(r2, True, p2)
                        total += tail_bundle[col].read(o2)
                        self.table._unpin_bundle(r2, True, p2, dirty=False)
                        continue
                    # No tail — read directly from base
                    if indirection != self.table.deleted:
                        total += bundle[col].read(offset)
                    self.table._unpin_bundle(range_id, False, page_id, dirty=False)
                else:
                    bundle = self.table._get_bundle(range_id, True, page_id)
                    total += bundle[col].read(offset)
                    self.table._unpin_bundle(range_id, True, page_id, dirty=False)
            return total
        except Exception:
            return False

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            total = 0
            found_any = False

            projection = [0] * self.table.num_columns
            projection[aggregate_column_index] = 1

            for key in range(start_range, end_range + 1):
                recs = self.select_version(key, self.table.key, projection, relative_version)

                if recs is False:
                    continue

                if len(recs) > 0:
                    found_any = True
                    value = recs[0].columns[aggregate_column_index]

                    if value is not None:
                        total += value

            return total if found_any else False
        except Exception:
            return False

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        recs = self.select(key, self.table.key, [1] * self.table.num_columns)
        if recs is False or len(recs) == 0:
            return False

        r = recs[0]
        updated_columns = [None] * self.table.num_columns
        updated_columns[column] = r.columns[column] + 1
        return self.update(key, *updated_columns)