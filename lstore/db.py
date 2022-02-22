from lstore.table import Table
from lstore.table import Bufferpool
import os
import json

BUFFER_POOL_SIZE = 100

class Database():

    def __init__(self):
        self.tables = {} 
        self.bufferpool = None
        self.path = ""
        pass

    def open(self, path):
        self.path = path
        # TODO: Create all the tables that already exist
        # TODO: Populate those tables with the page dir, rid dir
        # TODO: Create page ranges for all tables, and create its virtual pages and fill page IDs in virtual pages
        
        # open table directory file
        if os.path.exists(path + "/table_directory.json"):
            file = open(path + "/table_directory.json",)
            tables_file = json.load(file)

            # TESTING
            for name in tables_file:
                print(name)

        # Initialize the bufferpool
        self.bufferpool = Bufferpool(path)

        pass

    def close(self):
    
        # Write everything that's dirty in the Bufferpool to Disk
        self.bufferpool.write_all_to_disk()
        # create new dictionary for tables - contains num columns and key
        table_directory = {}
        # Add page directory and RID directory to disk
        for name, table in self.tables.items():
            page_dir_file_name = name + "_page_directory.json"
            rid_dir_file_name = name + "_rid_directory.json"

            # Page directory file write
            page_dir_file = open(self.path + "/" + page_dir_file_name, "w")
            json.dump(table.page_directory, page_dir_file)
            page_dir_file.close()

            # RID directory file write
            rid_dir_file = open(self.path + "/" + rid_dir_file_name, "w")
            json.dump(table.RID_directory, rid_dir_file)
            rid_dir_file.close()

            table_directory[name] = {"columns": table.num_columns, "key": table.key, "num_page_ranges": len(table.page_ranges)}

        # Table directory file write
        table_dir_file = open(self.path + "/table_directory.json", "w")
        json.dump(table_directory, table_dir_file)
        table_dir_file.close()

        
        # Delete the bufferpool
        self.bufferpool = None
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # table gets passed a bufferpool
        table = Table(name, num_columns, key_index, self.bufferpool)
        if (name not in self.tables):
            self.tables[name] = table # store table in database tables list
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        del self.tables[name] # remove table from dictionary
        pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
