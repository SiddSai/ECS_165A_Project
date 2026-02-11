from lstore.index import Index
from lstore.page import Page
from time import time

"""
The Table class provides the core of our relational storage functionality. All columns are 64-bit integers in this implementation. Users mainly interact with tables through queries. Tables provide a logical view of the actual physically stored data and mostly manage the storage and retrieval of data. Each table is responsible for managing its pages and requires an internal page directory that, given a RID, returns the actual physical location of the record. The table class should also manage the periodical merge of its corresponding page ranges.
"""

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
NULL_RID = -1



class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

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
        self.merge_threshold_pages = 50  # The threshold to trigger a merge

        self.next_rid = 0
        self.next_tail_rid = 0
        self.page_ranges = []

        self.base_pages = []
        self.tail_pages = []
        pass

    """
    if time: skeleton might expect insert to return True/False for success (method currently ends with pass). replace with a return (dependent on query)
    """
    def insert(self, record):
        # generate new rid
        rid = self.next_rid
        self.next_rid += 1

        # if no pages exist or current page is full, create new page
        if len(self.base_pages) == 0:
            bundle = [Page() for col in range(self.num_columns + 4)]
            self.base_pages.append(bundle)
        current = self.base_pages[-1]
        rid_page = current[RID_COLUMN]

        if not rid_page.has_capacity():
            bundle = [Page() for col in range(self.num_columns + 4)]
            self.base_pages.append(bundle)
            current = self.base_pages[-1]
            rid_page = current[RID_COLUMN]
        
        current[INDIRECTION_COLUMN].write(NULL_RID) # null for base page
        offset = rid_page.write(rid)
        current[TIMESTAMP_COLUMN].write(int(time()))
        current[SCHEMA_ENCODING_COLUMN].write(0) # no columns updated
        for col in range(self.num_columns):
            current[col + 4].write(record.columns[col])
        
        # update page directory with new rid and location
        page_id = len(self.base_pages) - 1
        range_id = 0 # until page ranges are implemented
        is_tail = False

        self.page_directory[rid] = (range_id, is_tail, page_id, offset)

        # update index with new rid and key value
        key_value = record.columns[self.key]
        self.index.insert(key_value, rid) # no insert method yet for index?
        pass

    # future: implement read_latest()
    def read(self, rid):
        # Retrieve record with the given RID by looking up the page directory and reading from the appropriate page
        if rid not in self.page_directory:
            return None
        range_id, is_tail, page_id, offset = self.page_directory[rid]
        if is_tail:
            bundle = self.tail_pages[page_id]
        else:
            bundle = self.base_pages[page_id]
        # metadata
        indirection = bundle[INDIRECTION_COLUMN].read(offset)
        stored_rid = bundle[RID_COLUMN].read(offset)
        timestamp = bundle[TIMESTAMP_COLUMN].read(offset)
        schema = bundle[SCHEMA_ENCODING_COLUMN].read(offset)
        # user columns
        columns = []
        for col in range(self.num_columns):
            value = bundle[col + 4].read(offset)
            columns.append(value)
        key_value = columns[self.key]

        return Record(rid, key_value, columns)
        

    def update(self, primary_key, columns):
        # Update the record with the given key by writing the updated values to a new tail page and updating the page directory and index accordingly.
        if len(columns) != self.num_columns:
            return False
        
        # find base rid using index on primary key
        base_rids = self.index.locate(self.key, primary_key)
        if not base_rids:
            return False
        base_rid = base_rids[0] # assume primary key is unique

        base_record = self.read(base_rid)
        if base_record is None:
            return False
        
        (range_id, is_tail, page_id, offset) = self.page_directory[base_rid]
        base_bundle = self.base_pages[page_id]
        latest_tail_rid = base_bundle[INDIRECTION_COLUMN].read(offset)
        if latest_tail_rid == NULL_RID:
            current_values = base_record.columns
        else:
            tail_record = self.read(latest_tail_rid)
            current_values = tail_record.columns

        # create new tail record with updated values
        new_values = current_values.copy()
        for i in range(self.num_columns):
            if columns[i] is not None:
                new_values[i] = columns[i]
        new_tail_rid = self.next_tail_rid
        self.next_tail_rid += 1
        
        # if no tail pages exist or current tail page is full, create new tail page
        tails = self.tail_pages
        if (len(tails) == 0) or (not tails[-1][RID_COLUMN].has_capacity()):
            bundle = [Page() for col in range(self.num_columns + 4)]
            tails.append(bundle)
        for col in range(self.num_columns):
            tails[-1][col + 4].write(new_values[col])
        tails[-1][INDIRECTION_COLUMN].write(base_rid)
        tail_offset = tails[-1][RID_COLUMN].write(new_tail_rid)
        tails[-1][TIMESTAMP_COLUMN].write(int(time()))
        tails[-1][SCHEMA_ENCODING_COLUMN].write(1) # columns updated

        tail_page_id = len(self.tail_pages) - 1
        self.page_directory[new_tail_rid] = (0, True, tail_page_id, tail_offset)

        base_record[INDIRECTION_COLUMN].write(new_tail_rid) # update indirection to point to latest tail record
        base_record[SCHEMA_ENCODING_COLUMN].write(base_record[SCHEMA_ENCODING_COLUMN].read(offset) | 1) # update schema encoding to indicate column updated
        pass

    def delete(self, rid):
        # Mark the record with the given RID as deleted by updating the appropriate page and page directory entry.
        pass

    # do last
    def __merge(self):
        print("merge is happening")
        pass
 
