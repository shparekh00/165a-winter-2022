
#M2
from lstore.page import Page
from lstore.disk import Disk
import math
from time import time

BUFFER_POOL_SIZE = 100

class Bufferpool:
    def __init__(self, path):
        self.pool_size = BUFFER_POOL_SIZE
        self.frames = [None] * self.pool_size # Frames contains page objects
        self.page_ids_in_bufferpool = [None] * self.pool_size
        self.access_times = [-math.inf] * self.pool_size
        self.pin_counts = [0] * self.pool_size
        self.disk = Disk(path)
        self.path = path
        pass
    
    '''
    Checks if there is an empty frame that we can put a page into
    '''
    def has_empty_frame(self):
        return (None in self.frames)

    '''
    Checks if page exists in bufferpool, if not gets if from
    Returns the page
    :param location: a Tuple (table_name, pr_id, virtual_page_id, page_id) to search for in the bufferpool frames
    '''
    def get_page(self, page_location):
        # print("Page location: ", page_location)
        # print("Page IDs: ", self.page_ids_in_bufferpool)
        if page_location in self.page_ids_in_bufferpool:
            # print("get_page(): Getting page from bufferpool")
            frame_index = self.page_ids_in_bufferpool.index(page_location)
            return self.frames[frame_index]
        else:
            # Get page from disk
            print("get_page(): Getting page from disk")
            new_page = self.read_from_disk(page_location)
            self.replace(new_page)
            return new_page
             
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
    Returns frame index of page we are evicting (Clock Replacement Policy - based on time accessed)
    '''

    # TODO: Make this into clock instead. No longer use access counts
    def get_eviction_frame_index(self):
        min_accessed = math.inf
        eviction_frame = None
        for frame_index, access_time in enumerate(self.access_times):
            # Also check pin count is 0 for page we evict
            if min_accessed > access_time and self.pin_counts[frame_index] == 0:
                min_accessed = access_time
                eviction_frame = frame_index

        # TODO: check that eviction_frame is not None since it is possible if all pin counts > 0
        # this may or may not be really important lol
        # print("eviction frame: ", eviction_frame)
        # print("access count: ", self.access_times[eviction_frame])
        if eviction_frame is None:
            print("all pin counts > 0, no eviction page chosen")
        return eviction_frame

    '''
    Replaces a frame in the bufferpool with page
    :param page: page we want to place in the bufferpool
    '''
    def replace(self, new_page):
        print("writing page to bufferpool")
        if not self.has_empty_frame():
            print("no empty frame")
            e_frame = self.get_eviction_frame_index()
            eviction_page = self.frames[e_frame]
            self.page_ids_in_bufferpool[e_frame] = None

            # We want to write to page if its dirty or we are closing DB
            if eviction_page.dirty == True:
                self.write_to_disk(eviction_page)

            # Set the frame to the new page & add page location to page_ids_in_bufferpool
            # Set access count, pin count at frame index to 0
            self.frames[e_frame] = new_page
            self.page_ids_in_bufferpool[e_frame] = new_page.location
            self.access_times[e_frame] = int(time())
            self.pin_counts[e_frame] = 0
        else:
            empty_frame_index = self.get_empty_frame_index()
            self.frames[empty_frame_index] = new_page
            self.page_ids_in_bufferpool[empty_frame_index] = new_page.location
        pass
        
    '''
    Anytime a page is accessed in the bufferpool, we need to pin it
    '''
    def pin_page(self, page_location):
        frame_index = self.page_ids_in_bufferpool.index(page_location) 
        self.access_times[frame_index] = int(time())
        self.pin_counts[frame_index] += 1

    def unpin_page(self, page_location):
        frame_index = self.page_ids_in_bufferpool.index(page_location)
        if self.pin_counts[frame_index] > 0:
            self.pin_counts[frame_index] -= 1

    '''
    We are only writing to disk when 
        (1): The bufferpool is evicting a dirty page, or
        (2): We are closing the DB so we need to write all dirty pages
    '''
    def write_to_disk(self, page):
        file_name = self.disk.create_file_name(page.location)
        # print("file being written to disk: ", file_name)
        page.dirty = False
        self.disk.write_to_disk(page, file_name)
    
    def write_all_to_disk(self):
        for pg in self.frames:
            if pg:
                if pg.dirty == True:
                    self.write_to_disk(pg)
                    pg.dirty = False
            

            
    '''
    Requests page from disk when 
        (1): The bufferpool does not have the page we are looking for
    Caller: get_page()
    '''
    def read_from_disk(self, page_location):
        return self.disk.retrieve_from_disk(page_location)


    
        