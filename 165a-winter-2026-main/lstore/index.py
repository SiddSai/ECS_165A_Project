"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table

        # Create indexes in key column
        self.indices[table.key] = {}

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # Check if does not exist
        if self.indices[column] is None:
            return []
        return self.indices[column].get(value, [])

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # Check if does not exist
        if self.indices[column] is None:
            return []
        result = []
        for value in self.indices[column]:
            if begin <= value <= end:
                result.extend(self.indices[column][value])

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices[column_number] = {}

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
