from logging import setLogRecordFactory
from lstore.virtualPage import *
from lstore.table import *
import copy

class basePage(virtualPage):

   def __init__(self, page_id, num_columns):
      super().__init__(page_id, num_columns)
      self.num_updates = 0 # reset to 0 after each merge
      self.tps = 0 # contains latest tail_rid of merge
      self.new_copy = 0 #TODO will contain a base page after merge, otherwise it will be 0
      self.new_copy_available = False
      self._callbacks = []
      
   def copy(self):
      return copy.deepcopy(self)
      
   # TODO remove 
   # @property
   # def new_copy_available(self):
   #      return self.new_copy_available

   # @new_copy_available.setter
   # def new_copy_available(self, value):
   #    self.new_copy_available = value
   #    self._notify_observers(self, self.new_copy, self.new_copy_available)

      # update base_page_copy to contain the new base_page
      #self.base_page_copy = new_value
      #update base page
      
      
      #do we do this by making a base page setter itself? like how to we use bpcopy to update the basepage?
      # update page dir to point to base_page_copy

   # stop main thread itll be done implicitly coz python is single threaded
   # basepage = copy
   #https://stackoverflow.com/questions/51885246/callback-on-variable-change-in-python

   # def _notify_observers(self, base_page, new_copy, new_copy_available):
   #    for callback in self._callbacks:
   #       callback(base_page, new_copy, new_copy_available)

   # def register_callback(self, callback):
   #    self._callbacks.append(callback)

   # TODO Move callback function to table and have it update base page with its new copy there, so that way we don't have to update self  
   # def look_for_update_to_base_page(base_page, new_copy, new_copy_available):
   #  if new_copy_available == True:
   #      base_page = new_copy



