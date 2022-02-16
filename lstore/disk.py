'''
Class that interacts with the data in files
'''
class Disk:
    # need to create a path
    def __init__(self):        
        self.files = {}
        pass 

    '''
    Open a new file named using table name and page range
    Return file_name
    '''
    def create_file(self, table_name, pr_id):
        file_name = table_name + str(pr_id) + ".json"
        key = (table_name, pr_id)
        # check if file name is already in list of files
        # if not open it and append to list
        if not self.files[key]:
            self.files[key] = file_name
        return file_name

    '''
    Writing a dirty page's content to file.
    :param page: The Page object we want to write to file.
    '''
    def write_to_file(self, page, file_name):
        file = open(file_name, "w")
        # TODO: how do we format what we are going to write to a file
        file.close()
        pass

    '''
    Read data from file
    '''
    def read_from_file(self, table_name, pr_id, virtual_page_id, page_id):
        pass

