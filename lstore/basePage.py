from lstore.virtualPage import *
from lstore.table import *

class basePage(virtualPage):

   def __init__(self, page_id, num_columns):
      super().__init__(page_id, num_columns)
      self.num_updates = 0 # reset to 0 after each merge
      self.tps = 0 # contains latest tail_rid of merge
      pass