'''
Class that interacts with the data in files
'''

import os
from os.path import exists
from lstore.page import Page

# python3 disk_test.py

class Disk:
    def __init__(self, path):   
        self.path = path
        # If directory doesn't exist, this makes a directory specified by user
        if not exists(path):
            os.mkdir(path)
        pass 

    '''
    Creating and returning file_name
    '''
    def create_file_name(self, page_location):
        # page_location = (table_name, pr_id, virtual_page_id, page_id)
        table_name = page_location[0]
        pr_id = page_location[1]
        virtual_page_id = page_location[2]
        page_id = page_location[3]

        # Example: Students-0-B_1-2.txt lol yuck
        file_name = table_name + "-" + str(pr_id) + "-" + virtual_page_id + "-" + str(page_id) + ".txt"
        key = page_location

        return file_name

    def set_disk_path(self, path):
        self.path = path
        if not exists(path):
            os.mkdir(path)

    '''
    Writing a dirty page's content to file.
    :param page: The Page object we want to write to file.
    '''
    def write_to_disk(self, page):
        file_name = self.create_file_name(page.location)
        file = open(self.path + "/" + file_name, "wb")

        file.write(page.data)
        file.close()

        pass

    '''
    Read data from file
    '''
    def retrieve_from_disk(self, page_location):
        ba = bytearray()
        file_name = self.create_file_name(page_location)

        file = open(self.path + "/" + file_name, "rb")
        for i in range(0, 512):
            ba += file.read(8)

        file.close()

        new_page = self.create_new_page(page_location)
        new_page.data = ba
       
        return new_page

    def create_new_page(self, page_location):
        table_name = page_location[0]
        pr_id = page_location[1]
        virtual_page_id = page_location[2]
        page_id = page_location[3]

        return Page(table_name, pr_id, virtual_page_id, page_id)

