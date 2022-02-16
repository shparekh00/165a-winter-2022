
#M2
from lstore.table import Page
import math

BUFFER_POOL_SIZE = 100

# TODO: might need to make this a singleton class so we don't create multiple bufferpools
# TODO: we need access to Page objects to write the contents to disk
# TODO: how should we format the data - JSON?
# page is uniquely identified by table, page range, virtual page id, page id
class Bufferpool:
    def __init__(self):
        self.frames = {} # key = page id, value = Frame
        self.pool_size = BUFFER_POOL_SIZE # we choose size of bufferpool based 
        pass

    def has_capacity(self):
        return len(self.frames) == self.pool_size

    # get page from disk and bring to bufferpool
    def replace(self):
        e_frame = self.get_eviction_page()

        
        # if page we are evicting is dirty, write back to disk
        pass
    
    # based on replacement policy, returns page_id of page we want to evict
    def get_eviction_page(self):
        # LRU
        min_accessed = math.inf
        eviction_frame = None
        for key, frame in self.frames:
            if min_accessed > frame.access_count:
                min_accessed = frame.access_count
                eviction_frame = key
                
        return eviction_frame
        
    def pin_page(self, page_id):
        frame = self.frames[page_id]
        frame.num_times_accessed += 1
        frame.pin_count += 1
        #increment pins on page

    def unpin_page(self, page_id):
        self.frames[page_id].pin_count -= 1

    # We are only writing to disk when (1): The bufferpool is evicting a dirty page and (2): We are closing the DB so we need to write all dirty pages
    def write_to_disk(self):
        pass

    


class Frame:
    def __init__(self, page_id):
        self.page_id = page_id
        self.access_count = 0
        self.pin_count = 0
        # TODO: Page Object
    
        