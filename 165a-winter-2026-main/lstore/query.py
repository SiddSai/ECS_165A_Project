from lstore.table import Table, Record
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
            # Milestone 1: ignore versioning, always return latest
            return self.select(search_key, search_key_index, projected_columns_index)
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
            total = 0
            found_any = False

            projection = [0] * self.table.num_columns
            projection[aggregate_column_index] = 1

            for key in range(start_range, end_range + 1):
                recs = self.select(key, self.table.key, projection)

                if recs is False:
                    return False

                if len(recs) > 0:
                    found_any = True
                    value = recs[0].columns[aggregate_column_index]

                    if value is not None:
                        total += value

            if found_any:
                return total
            else:
                return False

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
            # Milestone 1: ignore versioning, always return latest
            return self.sum(start_range, end_range, aggregate_column_index)
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
