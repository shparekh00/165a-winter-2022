from lstore.index import Index
from time import time
from lstore.pageRange import PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.key = key
        self.indirection = ""
        self.rid = rid
        self.timestamp = 0 #TODO
        self.schema_encoding = "0" * (len(columns) + 4)
        self.columns = columns # values of the rows in a list
        self.all_columns = [self.indirection, self.rid, self.timestamp, self.schema_encoding] # include metadata columns
        self.all_columns.append(self.columns)

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key      # indicates which column is primary key
        self.num_columns = num_columns + 4 # add 4 for the meta data columns

        # page_directory is a dictionary, key is RID, value is address (page range, page id, column)
        self.page_directory = {} # given a RID, returns the actual physical location of the record
        self.page_range_id = 0
        self.page_ranges = [PageRange(self.page_range_id, self.num_columns)]
        
        self.index = Index(self)
        pass


    def create_new_page_range(self):
        self.page_range_id += 1
        self.page_ranges.append(PageRange(self.page_range_id, self.num_columns))
        pass

    # not part of milestone 1
    def __merge(self):
        print("merge is happening")
        pass
 
