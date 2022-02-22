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


    def merge(self, base_page_copy, tail_RID):
        print("merge started")
        cols_merged = [0] * (base_page_copy.num_columns - 5) # tracks cols that have been merged already (set to 1 once updated)
        old_tps = base_page_copy.tps # merge starting here
        base_page_copy.tps = tail_RID # merge up to here


        for cur_tail_rid in range(tail_RID, old_tps, -1):
            # if all columns have been updated, stop merging
            if cols_merged.count(1) == len(cols_merged):
                break
            # find address of current tail range
            tail_rec_addy = self.page_directory[cur_tail_rid]
            tail_pr_id = tail_rec_addy["page_range_id"]
            tail_page_id = self.page_ranges[0].get_ID_int(tail_rec_addy["virtual_page_id"])
            tail_row = tail_rec_addy["row"]
            tail_page = self.page_ranges[tail_pr_id].tail_pages[tail_page_id]
            tail_sch_enc = bin(tail_page.pages[SCHEMA_ENCODING_COLUMN].read(tail_row))[2:].zfill(self.num_columns)
            # find address of corresponding base record
            tail_base_RID = tail_page.pages[BASE_RID_COLUMN].read(tail_row) # gets base RID
            base_page_addy = self.page_directory[tail_base_RID]
            base_page_row = base_page_addy["row"]
            # Find column with updated value in the tail page
            updated_col = next(x for x in tail_sch_enc if x == '1')
            # only merge latest columns
            if cols_merged[updated_col] == 1:
                continue
            # in-place update base copy
            base_page_copy.pages[updated_col+5].write(tail_page.pages[updated_col+5].read(tail_row), base_page_row)
            cols_merged[updated_col] = 1
        # Find base page address information
        base_pr_id = base_page_addy["page_range_id"]
        base_page_id = self.page_ranges[0].get_ID_int(base_page_addy["virtual_page_id"])
        #base_page_copy ready to be sent to main thread to replace base_page
        self.page_ranges[base_pr_id].base_pages[base_page_id].new_copy = base_page_copy
        #call set(base_page_copy(value, self.page_ranges[base_pr_id].base_pages[base_page_id]))
        self.page_ranges[base_pr_id].base_pages[base_page_id].new_copy_available = True
        print("merge finished")
        
        # #update the member variable to base_page_copy
        # self.page_ranges[base_pr_id].base_pages[base_page_id].base_page_copy = base_page_copy