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
        existing = self.index.locate(self.key, record.columns[self.key])
        if existing is not None:
            return False # duplicate key, insertion fails
        key_value = record.columns[self.key]
        self.index.insert(key_value, rid) # no insert method yet for index?
        return True

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

        # Follow indirection if not null and not deleted to get latest version of record
        if not is_tail and indirection != self.deleted and indirection != NULL_RID:
            return self.read(indirection)

        return Record(rid, key_value, columns)
        

    def update(self, rid, columns):
        # rid is the BASE rid (Query passes rid, *columns)
        if len(columns) != self.num_columns:
            return False

        # make sure rid exists
        if rid not in self.page_directory:
            return False

        (range_id, is_tail, page_id, offset) = self.page_directory[rid]

        # updates should target base records, not tail records
        if is_tail:
            return False

        base_rid = rid
        base_bundle = self.base_pages[page_id]

        base_record = self.read(base_rid)
        if base_record is None:
            return False

        prev_tail_rid = base_bundle[INDIRECTION_COLUMN].read(offset)
        if prev_tail_rid == NULL_RID:
            current_values = base_record.columns
        else:
            tail_record = self.read(prev_tail_rid)
            if tail_record is None:
                return False
            current_values = tail_record.columns

        # bit vector to track which columns are updated
        update_mask = 0
        for i in range(self.num_columns):
            if columns[i] is not None:
                update_mask |= (1 << i)
        if update_mask == 0:
            return False

        # create new tail record with updated values (cumulative tails)
        new_values = current_values.copy()
        for i in range(self.num_columns):
            if columns[i] is not None:
                new_values[i] = columns[i]

        new_tail_rid = self.next_rid
        self.next_rid += 1

        # if no tail pages exist or current tail page is full, create new tail page bundle
        tails = self.tail_pages
        if (len(tails) == 0) or (not tails[-1][RID_COLUMN].has_capacity()):
            bundle = [Page() for _ in range(self.num_columns + 4)]
            tails.append(bundle)

        # write new tail record (aligned: one write per column page)
        tail_bundle = self.tail_pages[-1]
        tail_offset = tail_bundle[RID_COLUMN].write(new_tail_rid)
        tail_bundle[INDIRECTION_COLUMN].write(prev_tail_rid)  # point to previous tail record
        tail_bundle[TIMESTAMP_COLUMN].write(int(time()))
        tail_bundle[SCHEMA_ENCODING_COLUMN].write(update_mask)

        for col in range(self.num_columns):
            tail_bundle[col + 4].write(new_values[col])

        tail_page_id = len(self.tail_pages) - 1
        self.page_directory[new_tail_rid] = (0, True, tail_page_id, tail_offset)

        # update base metadata in-place
        base_bundle[INDIRECTION_COLUMN].update(offset, new_tail_rid)
        old_schema = base_bundle[SCHEMA_ENCODING_COLUMN].read(offset)
        base_bundle[SCHEMA_ENCODING_COLUMN].update(offset, old_schema | update_mask)

        return True



    deleted = -5
    def delete(self, rid):
        # Mark the record with the given RID as deleted by updating the appropriate page and page directory entry.
        if rid not in self.page_directory:
            return False
        
        # Get location
        range_id, is_tail, page_id, offset = self.page_directory[rid]

        # Get page bundle
        pg_bundle =  self.tail_pages[page_id] if is_tail else self.base_pages[page_id]

        # Read key value for index deletion
        key_value = pg_bundle[self.key + 4].read(offset)

        # Mark as deleted with deletion flag
        pg_bundle[INDIRECTION_COLUMN].update(offset, self.deleted)

        # Remove from index
        self.index.delete(key_value, rid)

        # Remove page directory entry
        del self.page_directory[rid]

        return True

    # do last
    def __merge(self):
        print("merge is happening")
        limit = self.merge_threshold_pages
        # check if merge is needed
        pass
 
