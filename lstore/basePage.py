from logging import setLogRecordFactory
from lstore.virtualPage import *
from lstore.table import *
import copy

class basePage(virtualPage):

   def __init__(self, page_id, num_columns):
      super().__init__(page_id, num_columns)
      self.num_updates = 0 # reset to 0 after each merge
      self.tps = 0 # contains latest tail_rid of merge
      self.base_page_copy = 0 # will contain a base page after merge, otherwise it will be 0
      pass

   def copy(self):
      return copy.deepcopy(self)