from lstore.virtualPage import *
from lstore.table import *

class tailPage(virtualPage):

   def __init__(self, table_name, pr_id, page_id, num_columns):
      super().__init__(table_name, pr_id, page_id, num_columns)
      pass