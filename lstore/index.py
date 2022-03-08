"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

INDIRECTION_COLUMN = 0
RID_COLUMN = 1 
TIMESTAMP_COLUMN = 2 
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.table = table 
        self.indices = [{}] * (self.table.num_columns)
        # self.indices[0] = {} # initialize primary key index

    def has_index(self, column):
        #print(self.indices[column])
        if self.indices[column] != None:
            return True
        else:
            return False

    def insert_record(self, column, value, rid):
        if self.has_index(column):
            hashtable = self.indices[column]
            if value in hashtable:
                hashtable[value].append(rid)
            else:
                hashtable[value] = [rid]
 

    def update_record(self, column, old_value, value, old_rid, rid):
        if self.has_index(column):
            hashtable = self.indices[column]
            self.delete_record(column, old_value, old_rid)
            self.insert_record(column, value, rid)
         

    def delete_record(self, column, value, rid):
       if self.has_index(column):
            hashtable = self.indices[column]
            #print(hashtable[value])
            if rid in hashtable[value]: 
                #index = hashtable[value].index(rid)
                #print(index)
                hashtable[value].remove(rid)
        

    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):
        if self.has_index(column):
            #print("Column: ", self.indices[column])
            return self.indices[column][value]
        else:
            print("No index on column")
            return False
            

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    :param begin/end: the range we want to check from
    :param column: the column we search for
    """
    def locate_range(self, begin, end, column): 
        ret_list = []
        
        # Iterate thru every value between begin and end and check if it exists
        if not self.has_index(column):
            print("No index on column")
            return 
        hashtable = self.indices[column]
        for val in range(begin, end+1):
            if val in hashtable:
                for rid_idx, rid in enumerate(hashtable[val]):
                    ret_list.append(hashtable[val][rid_idx])
             
        return ret_list

    """
    # Create index/hashtable on specified column
    """
    def create_index(self, column_number):
        if self.has_index(column_number):
            pass
        else:
            self.indices[column_number] = {}
        pass

    """
    # Drop index/hashtable of specified column
    """
    def drop_index(self, column_number):
        if column_number == 0:
            pass
        else:
            self.indices[column_number] = None
        pass
