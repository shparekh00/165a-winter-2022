# Parent class
# Either a base or tail page

from lstore.page import Page

class virtualPage:
    # init for both base and tail pages
    def __init__(self, table_name, pr_id, page_id, num_columns):
        self.page_id = page_id  # page id is given by page range class
        self.num_columns = num_columns # num columns is given by page range class
        self.pages = [] # array of physical pages, one for each column 
        # create x (blank) pages for x cols
        for i in range(0,num_columns):
            self.pages.append(Page(table_name, pr_id, page_id, i)) 



    def has_capacity(self):
        return self.pages[0].has_capacity()

    def insert_record(self, record, row=None):
        #print(record.all_columns)
        for i in range(0, self.num_columns):
            try:
                self.pages[i].write(record.all_columns[i], row)
            except Exception:
                print(i)
                print(record.all_columns[i])
                print("failed insert_record")
                # failing when we try to insert a string