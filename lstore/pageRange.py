import re
#from typing_extensions import Self
from lstore.basePage import *
from lstore.tailPage import *


PAGE_RANGE_SIZE = 524288  # byte capacity for page range (512K) - each page range holds 16 virtual pages
PAGE_SIZE = 4096 # physical page size

class PageRange:
    def __init__(self, table_name, id, num_columns):
        # last page in these arrays is the one that's active
        self.table_name = table_name
        self.pr_id = id

        # page size in bytes, one physical page for each column
        # num columns includes metadata columns

        self.virtual_page_size = num_columns * PAGE_SIZE # bytes in virtual page
        
        # Virtual page ids (current ID?)
        self.base_page_id = "B_0"
        self.tail_page_id = "T_0"
        self.num_columns = num_columns

<<<<<<< HEAD
        self.base_pages = [virtualPage(self.table_name, self.pr_id, self.base_page_id, num_columns)]
        self.tail_pages = [virtualPage(self.table_name, self.pr_id, self.tail_page_id, num_columns)]
=======
        self.base_pages = [basePage(self.base_page_id, num_columns)]
        self.tail_pages = [tailPage(self.tail_page_id, num_columns)]
>>>>>>> remotes/origin/Merging

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
        
    # if tail pages are full but we need to update, we will have to do a merge
    def has_capacity(self):
        return len(self.base_pages) + len(self.tail_pages) < (PAGE_RANGE_SIZE // self.virtual_page_size)

    def base_pages_has_capacity(self):
        return self.base_pages[-1].has_capacity()
        

    # returns true if new page successfully created
    # deprecated
    def add_tail_page(self):
<<<<<<< HEAD
        print("this function is deprecated")
        # if self.has_capacity():
        #     self.increment_tailpage_id()
        #     self.tail_pages.append(virtualPage(self.tail_page_id, self.num_columns))
        #     return True
        # else:
        #     return False
=======
        
        #check if num_updates == limit_for_merging:
            # num_updates = 0
            # create thread(__merge)
            # thread = threading.Thread(target=__merge, args=())
        if self.has_capacity():
            self.increment_tailpage_id()
            self.tail_pages.append(tailPage(self.tail_page_id, self.num_columns))
            return True
        else:
            return False
>>>>>>> remotes/origin/Merging
            
    # returns true if new page successfully created
    # deprecated
    def add_base_page(self):
<<<<<<< HEAD
        print("this function is deprecated")
        # if self.has_capacity(): # checks that page range has capacity
        #     self.increment_basepage_id()
        #     self.base_pages.append(virtualPage(self.base_page_id, self.num_columns))
        #     return True
        # else:
        #     return False
=======
        if self.has_capacity(): # checks that page range has capacity
            self.increment_basepage_id()
            self.base_pages.append(basePage(self.base_page_id, self.num_columns))
            return True
        else:
            return False
>>>>>>> remotes/origin/Merging
