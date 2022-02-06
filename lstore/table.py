from lstore.index import Index
from time import time
from lstore.pageRange import PageRange
#from bitstring import BitArray

INDIRECTION_COLUMN = 0 # changed upon updating (TAIL)
RID_COLUMN = 1 # set upon creating record
TIMESTAMP_COLUMN = 2 # set upon creating record
SCHEMA_ENCODING_COLUMN = 3 # changed upon updating (TAIL)


class Record:

    def __init__(self, rid, key, columns):
        self.key = key
        self.indirection = 0 #TODO TURN INTO INTEGER, 0 means uninitialized, -1 means deleted
        self.rid = rid
        self.timestamp = 0 #TODO
        # self.schema_encoding = "0" * (len(columns) + 4) #TODO BITARRAY
        self.schema_encoding = 0 # store as int to save space. convert to binary before using. all 1's are updated. 0's are not
        self.columns = columns # user values for record passed in as a tuple (spans multiple columns)
        self.all_columns = [self.indirection, self.rid, self.timestamp, self.schema_encoding] # metadata values
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
        self.num_columns = num_columns + 4 # add 4 for the meta data columns

        # page_directory is a dictionary, key is RID, value is address (page range, #TODO(BASE?) page id, column) & primary key
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
 
