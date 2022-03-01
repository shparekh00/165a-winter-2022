from index import Index
from time import time
from pageRange import PageRange
from basePage import *
from tailPage import *
from virtualPage import virtualPage
from bufferpool import Bufferpool
from page import Page
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

   
# TODO: Start implementing indexing
# TODO: Add create_index and drop_index into table class instead

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, bufferpool=None):
        self.name = name
        self.key = key      # indicates which column is primary key
        #self.num_columns = num_columns + 4 # add 4 for the meta data columns
        self.num_columns = num_columns
        # page_directory is a dictionary, key is RID, value is address (page range, #TODO(BASE?) page id, column) & primary key
        self.page_directory = {} # given a RID, returns the actual physical location of the record
        self.RID_directory = {} # given a primary key, returns the RID of the record
        self.page_range_id = -1
        self.RID_counter = -1
        self.page_ranges = []
        self.bufferpool = bufferpool
        self.index = Index(self)
        self.create_new_page_range()
        pass
    

    def has_capacity_page(self, page_location):
        page = self.access_page_from_memory(page_location)
        return page.has_capacity()

    def create_new_page_range(self):
        # TODO: When we are repoopulating tables, self.add_pages_to_disk will give NoneType error
        self.page_range_id += 1
        self.page_ranges.append(PageRange(self.name, self.page_range_id, self.num_columns+5))

        # insert all the physical pages of this page range into bufferpool
        tail_page = self.page_ranges[-1].tail_pages[-1]
        base_page = self.page_ranges[-1].base_pages[-1]

        # Add pages to disk
        # self.add_pages_to_bufferpool(base_page.pages)
        # self.add_pages_to_bufferpool(tail_page.pages)
        self.add_pages_to_disk(base_page.pages)
        self.add_pages_to_disk(tail_page.pages)
        pass
    
    def add_tail_page(self, pr_id):
        page_range = self.page_ranges[pr_id]

        # Check if the page range has capacity
        if page_range.has_capacity():
            page_range.increment_tailpage_id()
            page_range.tail_pages.append(tailPage(self.name, page_range.pr_id, page_range.tail_page_id, self.num_columns+5))
            # Add pages to bufferpool
            tail_page = page_range.tail_pages[-1]
            #self.add_pages_to_bufferpool(tail_page.pages)
            self.add_pages_to_disk(tail_page.pages)
            return True
        else:
            return False
        pass

    def add_base_page(self, pr_id):
        page_range = self.page_ranges[pr_id]

        # Check if the page range has capacity
        if page_range.has_capacity():
            page_range.increment_basepage_id()
            virt_page = basePage(self.name, page_range.pr_id, page_range.base_page_id, self.num_columns+5)
            page_range.base_pages.append(virt_page)
 
            # Add pages to bufferpool
            base_page = page_range.base_pages[-1]
            # self.add_pages_to_bufferpool(base_page.pages)
            self.add_pages_to_disk(base_page.pages)
            return True
        else:
            return False
        pass

    # TODO: Currently not used
    def add_pages_to_bufferpool(self, pages):
        for page_location in pages:
            table_name = page_location[0]
            pr_id = page_location[1]
            vp_id = page_location[2]
            page_id = page_location[3]
            new_page = Page(table_name, pr_id, vp_id, page_id)
            self.bufferpool.replace(new_page)
        pass

    def add_pages_to_disk(self, pages):
        for page_location in pages:
            table_name = page_location[0]
            pr_id = page_location[1]
            vp_id = page_location[2]
            page_id = page_location[3]
            new_page = Page(table_name, pr_id, vp_id, page_id)
            self.bufferpool.write_to_disk(new_page)
        pass

    # TODO: retrieve_page_from_memory
    def access_page_from_memory(self, page_location_info):
        accessed_page = self.bufferpool.get_page(page_location_info)
        self.bufferpool.pin_page(page_location_info)
        return accessed_page

    def finish_page_access(self, page_location_info):
        self.bufferpool.unpin_page(page_location_info)

    def create_new_RID(self):
        # first RID will be 0
        self.RID_counter += 1
        return self.RID_counter

    def insert_record(self, virtual_page, record, row=None):
        for i in range(0, self.num_columns+5):
            try:
                page = self.access_page_from_memory(virtual_page.pages[i])
                page.write(record.all_columns[i], row)
                # print("finished writing")
                page.dirty = True
                self.bufferpool.set_page_dirty(page)
                self.finish_page_access(virtual_page.pages[i])

            except Exception:
                #print("failed insert_record on page ", i)
                pass
                # failing when we try to insert a string


    def merge(self, base_page_copy, tail_RID):

        print("merge started")
        cols_merged = [0] * (base_page_copy.num_columns - 5) # tracks cols that have been merged already (set to 1 once updated)
        old_tps = base_page_copy.tps # merge starting here
        base_page_copy.tps = tail_RID # merge up to here

        if old_tps > tail_RID:
            print("something's wrong in merge()")
        for cur_tail_rid in range(tail_RID, old_tps, -1):
            # if all columns have been updated, stop merging
            if cols_merged.count(1) == len(cols_merged):
                break
            # find address of current tail page
            tail_rec_addy = self.page_directory[cur_tail_rid]
            tail_pr_id = tail_rec_addy["page_range_id"]
            tail_page_id = self.page_ranges[0].get_ID_int(tail_rec_addy["virtual_page_id"]) 
            tail_row = tail_rec_addy["row"]
            ## FIXME
            #print(cur_tail_rid)
            try:
                pr = self.page_ranges[tail_pr_id]
                tp = pr.tail_pages[tail_page_id]
            except:
                print("MERGE EXECPTION")
                print("cur_tail_rid: ", cur_tail_rid)
                print("tail_pr_id: ", tail_pr_id)
                print("tail_page_id: ", tail_page_id)
                print("tail_row", tail_row)
                print("num of tail pages: ", len(pr.tail_pages))
                return 0

            tail_page = tp
            # tail_page = self.page_ranges[tail_pr_id].tail_pages[tail_page_id]
            ## FIXME
            tail_schema_page = self.access_page_from_memory(tail_page.pages[SCHEMA_ENCODING_COLUMN])
            tail_sch_enc = bin(tail_schema_page.read(tail_row))[2:].zfill(self.num_columns)
            # find address of corresponding base record
            # TODO
            tail_base_RID = self.access_page_from_memory(tail_page.pages[BASE_RID_COLUMN]).read(tail_row) # gets base RID
            self.finish_page_access(tail_page.pages[BASE_RID_COLUMN])
            base_page_addy = self.page_directory[tail_base_RID]
            base_page_row = base_page_addy["row"]
            # Find column with updated value in the tail page
            ####
            # updated_col = next(x for x in tail_sch_enc if x == '1')
            updated_col = -1 # should always be updated in for loop
            for i, x in enumerate(tail_sch_enc):
                if tail_sch_enc[i] == '1':
                    updated_col = i
                    break
            ####
            # only merge latest columns
            if cols_merged[updated_col] == 1:
                continue
            # in-place update base copy
            # TODO
            access_page = self.access_page_from_memory(base_page_copy.pages[updated_col+5])
            access_page2 = self.access_page_from_memory(tail_page.pages[updated_col+5])
            access_page.write(access_page2.read(tail_row), base_page_row)
            cols_merged[updated_col] = 1
            self.finish_page_access(base_page_copy.pages[updated_col+5])
            self.finish_page_access(tail_page.pages[updated_col+5])
        # Find base page address information
        base_pr_id = base_page_addy["page_range_id"]
        base_page_id = self.page_ranges[0].get_ID_int(base_page_addy["virtual_page_id"])
        #base_page_copy ready to be sent to main thread to replace base_page
        self.page_ranges[base_pr_id].base_pages[base_page_id].new_copy = base_page_copy
        #call set(base_page_copy(value, self.page_ranges[base_pr_id].base_pages[base_page_id]))
        self.page_ranges[base_pr_id].base_pages[base_page_id].new_copy_available = True
       #print("merge finished")
        # #update the member variable to base_page_copy
        # self.page_ranges[base_pr_id].base_pages[base_page_id].base_page_copy = base_page_copy
