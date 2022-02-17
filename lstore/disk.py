'''
Class that interacts with the data in files
'''
from lstore import Page
class Disk:
    # need to create a path
    def __init__(self, path):   
        # TODO: might just not bc it will get erased anyways when db closes so we can just do try catch when we open a file     
        self.path = path
        self.files = {}
        pass 

    '''
    Creating and returning file_name
    '''
    def create_file_name(self, page_location):
        # (table_name, pr_id, virtual_page_id, page_id)
        table_name = page_location.location[0]
        pr_id = page_location.location[1]
        virtual_page_id = page_location.location[2]
        page_id = page_location.location[3]

        #Students-0-B_1-2.txt lol yuck
        file_name = table_name + "-" + str(pr_id) + "-" + virtual_page_id + "-" + str(page_id) + ".txt"
        key = page_location

        # Check if file name is already in list of files
        # if not open it and append to list
        if not key in self.files:
            self.files[key] = file_name
        return file_name

    '''
    Writing a dirty page's content to file.
    :param page: The Page object we want to write to file.
    '''
    # TODO : what if diretory doesn't exist
    def write_to_disk(self, page, file_name):
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
        pass

    def create_new_page(self, page_location):
        # (table_name, pr_id, virtual_page_id, page_id)
        table_name = page_location.location[0]
        pr_id = page_location.location[1]
        virtual_page_id = page_location.location[2]
        page_id = page_location.location[3]

        return Page(table_name, pr_id, virtual_page_id, page_id)

