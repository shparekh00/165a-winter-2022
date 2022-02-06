from lstore.virtualPage import *
from lstore.table import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class basePage(virtualPage):
    pass
    # self.page_id = page_id  # page id is given by page range class
    # self.num_columns = num_columns # num columns is given by page range class
    # self.pages = [] # array of physical pages, one for each column 

    def __init__(self, page_id, num_columns):
       super().__init__(page_id, num_columns)
    #    self.indirection_col = self.pages[INDIRECTION_COLUMN]
    #    self.schema_encoding_col = self.pages[SCHEMA_ENCODING_COLUMN]
    
    # # not sure what we would use this for
    # def indirection_col_update_val(self, new_val):
    #     self.indirection_col.write(new_val)
    #     pass
