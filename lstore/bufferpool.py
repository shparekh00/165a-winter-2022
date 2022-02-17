
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
    def __init__(self, path):
        self.pool_size = BUFFER_POOL_SIZE # we choose size of bufferpool
        # frames contains page objects
        self.frames = [None] * self.pool_size # TODO: how to initialize a list with a bunch of None values
        self.page_ids_in_bufferpool = []
        self.access_counts = [0] * self.pool_size #index = frame, value = count of times the frame was accessed - need to reset the count to 0 when the frame is replaced
        self.pin_counts = [0] * self.pool_size
        self.disk = Disk(path)
        self.path = path
        pass
    
    '''
    Checks if there is an empty frame that we can put a page into
    '''
    def has_empty_frame(self):
        return self.frames.index(None) != -1
        #return len(self.frames) < self.pool_size

    '''
    Checks if page exists in bufferpool
    Returns the index where the page is found
    :param location: a Tuple (table_name, pr_id, virtual_page_id, page_id) to search for in the bufferpool frames
    '''
    def get_page(self, page_location):
        frame_index = self.frames.index(page_location)
        if frame_index != -1:
            return self.frames[frame_index]
        else:
            # Get page from disk
            # self.read_from_disk()
            pass 
        pass



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
 
        if not self.has_empty_frame():
            e_frame = self.get_eviction_page()
            #evict
            # self.write_to_disk()
            # set access count at frame index to 0
            # set pin count at frame index to 0
        else:
            empty_frame_index = self.get_empty_frame_index()
            self.frames[empty_frame_index] = page
            self.page_ids_in_bufferpool.append(page.location)
        pass

            
    '''
    Returns frame index of page we are evicting (LRU Replacement Policy)
    '''
    def get_eviction_page(self):
        # Check pin count is 0 for page we evict
        min_accessed = math.inf
        eviction_frame = None
        for frame_index, access_count in enumerate(self.access_counts):
            if min_accessed > access_count and self.pin_counts[frame_index] == 0:
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
    def write_to_disk(self, page):
        file_name = self.disk.create_file_name(page.location)
        self.disk.write_to_disk(page, file_name)
        pass
    
    '''
    Requests page from disk when 
        (1): The bufferpool does not have the page we are looking for

    Caller: get_page()
    '''
    def read_from_disk(self, page_location):
        self.disk.retrieve_from_disk(page_location)
        #
        pass







# #TODO: do we use this
# class Frame:
#     def __init__(self, page_id):
#         self.page_id = page_id
#         self.access_count = 0
#         self.pin_count = 0
#         # TODO: Page Object
    
        