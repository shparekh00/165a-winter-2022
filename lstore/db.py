from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = {} # list of tables, changed it to a dictionary?
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
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
