from lstore.index import Index
from time import time
from lstore.pageRange import PageRange
#import bitarray

INDIRECTION_COLUMN = 0
# -1 = record has been deleted
## for base pages: init to 0, point to a tail RID after an update
## for tail pages: init to 0, point to a tail RID of previous update
RID_COLUMN = 1 
# init to an integer (starting from 0)
TIMESTAMP_COLUMN = 2 
# init to time of record creation
SCHEMA_ENCODING_COLUMN = 3
## for base pages: init to 0, 1 after an update (per column)
## for tail pages: init to col # that contains updated value. ex: [none, none, 7, none] -> schema = 2
BASE_RID_COLUMN = 4
## for base pages: set to -1, not used in base pages, but kept for consistency
## for tail pages: set to corresponding base (original) record


class Record:

    def __init__(self, rid, key, columns, select = False, base_rid = -1):
        self.all_columns = []
        # If we are creating records from our select query, we don't need to include any metadata
        if not select:
            self.indirection = 0
            self.rid = rid
            self.schema_encoding = 0 # convert to binary when we update it and use bitwise OR (look in query update)
            self.timestamp = int(time()) #TODO
            self.base_rid = base_rid
            self.all_columns = [self.indirection, self.rid, self.timestamp, self.schema_encoding, self.base_rid] # metadata values

        self.key = key
        self.columns = columns # user values for record passed in as a tuple (spans multiple columns)
        self.all_columns += self.columns

   
        

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key      # indicates which column is primary key
        #self.num_columns = num_columns + 4 # add 4 for the meta data columns
        self.num_columns = num_columns
        # page_directory is a dictionary, key is RID, value is address (page range, #TODO(BASE?) page id, column) & primary key
        self.page_directory = {} # given a RID, returns the actual physical location of the record
        self.RID_directory = {} # given a primary key, returns the RID of the record
        self.page_range_id = 0
        self.RID_counter = -1
        self.page_ranges = [PageRange(self.page_range_id, self.num_columns+5)]
        
        self.index = Index(self)
        pass

    def create_new_page_range(self):
        self.page_range_id += 1
        self.page_ranges.append(PageRange(self.page_range_id, self.num_columns+5))
        pass

    def create_new_RID(self):
        # first RID will be 0
        self.RID_counter += 1
        return self.RID_counter

    def merge(self):
        num = 0
        print("started")
        for i in range(0, 100):
            num += i
        print("merge is done")
        pass