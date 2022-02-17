from lstore.table import Table
from lstore.table import Bufferpool

BUFFER_POOL_SIZE = 100

class Database():

    def __init__(self):
        self.tables = {} # list of tables, changed it to a dictionary?
        self.bufferpool = None
        pass

    def open(self, path):
        # Initialize the bufferpool
        self.bufferpool = Bufferpool(path)
        pass

    def close(self):
    
        # Write everything that's dirty in the Bufferpool to Disk

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
