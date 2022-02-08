import re
#from typing_extensions import Self
from lstore.basePage import *
from lstore.tailPage import *


PAGE_RANGE_SIZE = 524288  # byte capacity for page range (256K) - each page range holds 16 virtual pages
PAGE_SIZE = 4096 # physical page size

class PageRange:
    def __init__(self, id, num_columns):
        # last page in these arrays is the one that's active
        self.pr_id = id
        # page size in bytes, one physical page for each column
        # num columns includes metadata columns
        self.virtual_page_size = num_columns * PAGE_SIZE # bytes in virtual page
        # Virtual page ids (current ID?)
        self.base_page_id = "B_0"
        self.tail_page_id = "T_0"
        self.num_columns = num_columns

        self.base_pages = [basePage(self.base_page_id, num_columns)]
        self.tail_pages = [tailPage(self.tail_page_id, num_columns)]

    def increment_basepage_id(self):
        id = self.base_page_id.split('_') # ex: id = ["B", "1"]
        num = int(id[-1])
        num += 1
        self.base_page_id = "B_" + str(num)
        pass
        
    def increment_tailpage_id(self):
        id = self.tail_page_id.split('_')
        num = int(id[-1])
        num += 1
        self.tail_page_id = "T_" + str(num)
        pass

    def get_ID_int(self, id):
        id = id.split('_')
        return int(id[-1])
        
    
    def has_capacity(self):
        return len(self.base_pages) + len(self.tail_pages) < (PAGE_RANGE_SIZE // self.virtual_page_size)

    def base_pages_has_capacity(self):
        return self.base_pages[-1].has_capacity()
        

    # returns true if new page successfully created
    def add_tail_page(self):
        if self.has_capacity():
            self.increment_tailpage_id()
            self.tail_pages.append(tailPage(self.tail_page_id, self.num_columns))
            return True
        else:
            return False
            
    # returns true if new page successfully created
    def add_base_page(self):
        if self.has_capacity(): # checks that page range has capacity
            self.increment_basepage_id()
            self.base_pages.append(basePage(self.base_page_id, self.num_columns))
            return True
        else:
            return False
