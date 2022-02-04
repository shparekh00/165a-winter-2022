import re
from typing_extensions import Self
from lstore.virtualPage import *


PAGE_RANGE_SIZE = 65536  # byte capacity for page range (64K)
PAGE_SIZE = 4096

class PageRange:
    def __init__(self, id, num_columns):
        # last page in these arrays is the one that's active
        self.pr_id = id
        # page size in bytes, one physical page for each columnw
        self.page_size = num_columns 
        # Virtual page ids
        self.tail_page_id = "T_1"
        self.base_page_id = "B_1"

        self.base_pages = [virtualPage(self.base_page_id, num_columns)]
        self.tail_pages = [virtualPage(self.tail_page_id, num_columns)]

    def increment_basepage_id(self):
        id = self.base_page_id.split('_')
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
        
    
    def has_capacity(self):
        return len(self.base_pages) + len(self.tail_pages) < (PAGE_RANGE_SIZE / PAGE_SIZE)
        

    # returns true if new page successfully created
    def add_tail_page(self):
        if self.has_capacity():
            self.increment_tailpage_id()
            self.tail_pages.append(virtualPage(self.tail_page_id, self.page_size))
            return True
        else:
            return False
            
    # returns true if new page successfully created
    def add_base_page(self):
        if self.has_capacity():
            self.increment_basepage_id()
            self.base_pages.append(virtualPage(self.base_page_id, self.page_size))
            return True
        else:
            return False
