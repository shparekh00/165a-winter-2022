
#M2
from lstore import Page
from lstore import Disk
import math

BUFFER_POOL_SIZE = 100

# TODO: might need to make this a singleton class so we don't create multiple bufferpools
# TODO: we need access to Page objects to write the contents to disk
# TODO: how should we format the data - JSON?
# page is uniquely identified by table, page range, virtual page id, page id
class Bufferpool:
    def __init__(self):
        self.pool_size = BUFFER_POOL_SIZE # we choose size of bufferpool
        # frames contains page objects
        self.frames = [None] * self.pool_size # TODO: how to initialize a list with a bunch of None values
        self.access_counts = [0] * self.pool_size #index = frame, value = count of times the frame was accessed - need to reset the count to 0 when the frame is replaced
        self.pin_counts = [0] * self.pool_size
        self.disk = Disk()
        pass

    def has_empty_frame(self):
        return len(self.frames) == self.pool_size

    '''
    Returns the index of an empty frame
    '''
    def get_empty_frame_index(self):
        for i, frame in enumerate(self.frames):
            if frame == None:
                return i
        
        print("No empty frames, you must evict a page first")

    '''
    Replaces a frame in the bufferpool with page
    :param page: page we want to place in the bufferpool
    '''
    def replace(self, page):
        # if bufferpool is full
        #   get eviction page
        #   replace eviction page with page we want to put in
        # else
        #   place page in empty bufferpool frame
 
        if not self.has_empty_frame:
            e_frame = self.get_eviction_page()
            #evict
            # self.write_to_disk()
        else:
            empty_frame_index = self.get_empty_frame_index()
            self.frames[empty_frame_index] = page
        pass

            
    '''
    Returns frame index of page we are evicting (LRU Replacement Policy)
    '''
    def get_eviction_page(self):
        min_accessed = math.inf
        eviction_frame = None
        for frame_index, access_count in enumerate(self.access_counts):
            if min_accessed > access_count:
                min_accessed = access_count
                eviction_frame = frame_index
        return eviction_frame
        
    '''
    Anytime a page is accessed in the bufferpool, we need to pin it
    '''
    def pin_page(self, frame_index):
        self.access_counts[frame_index] += 1
        self.pin_counts[frame_index] += 1

    def unpin_page(self, frame_index):
        if self.pin_counts[frame_index] > 0:
            self.pin_counts[frame_index] -= 1

    '''
    We are only writing to disk when 
        (1): The bufferpool is evicting a dirty page, or
        (2): We are closing the DB so we need to write all dirty pages
    '''
    def write_to_disk(self):
        
        pass
    
    '''
    We are reading from disk when 
        (1): The bufferpool does not have the page we are looking for
    '''
    def read_from_disk(self):
        
        pass







# #TODO: do we use this
# class Frame:
#     def __init__(self, page_id):
#         self.page_id = page_id
#         self.access_count = 0
#         self.pin_count = 0
#         # TODO: Page Object
    
        