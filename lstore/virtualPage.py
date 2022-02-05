# Parent class
# Either a base or tail page

from lstore.page import Page

class virtualPage:
    # init for both base and tail pages
    def __init__(self, page_id, num_columns):
        self.page_id = page_id  # page id is given by page range class
        self.num_columns = num_columns # num columns is given by page range class
        self.pages = [] # array of physical pages, one for each column 
        for i in range(0,num_columns):
            self.pages.append(Page(i)) # we're passing in the column the phys page are part of


    def has_capacity(self):
        return self.pages[0].has_capacity()

    def insert_record(self, record):
        for i in range(0, self.num_columns):
            self.pages[i].write(record.all_columns[i])