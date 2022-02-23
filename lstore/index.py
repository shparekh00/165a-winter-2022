"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

INDIRECTION_COLUMN = 0
RID_COLUMN = 1 
TIMESTAMP_COLUMN = 2 
SCHEMA_ENCODING_COLUMN = 3

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.table = table 
        self.indices = [None] * (self.table.num_columns)
        self.indices[0] = {} # initialize primary key index

    def insert_record(self, column, value, rid):
        hashtable = self.indices[column]
        if hashtable != None:
            #print("inserting in hashtable")
            if value in hashtable:
                hashtable[value].append(rid)
            else:
                hashtable[value] = [rid]
 

    def update_record(self, old_value, value, old_rid, rid):
        self.delete_record(old_value, old_rid)
        self.insert_record(value, rid)
        

    def delete_record(self, column, value, rid):
        hashtable = self.indices[column]
        if hashtable != None:
            print(hashtable[value])

            index = hashtable[value].index(rid)
            print(index)
            hashtable[value].remove(index)
        

    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):
        if self.indices[column] != None:
            return self.indices[column][value]
        else:
            print("No index on column")
            

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    :param begin/end: the range we want to check from
    :param column: the column we search for
    """
    def locate_range(self, begin, end, column): 
        ret_list = []
        
        # Iterate thru every value between begin and end and check if it exists
        hashtable = self.indices[column]
        if hashtable == None:
            print("No index on column")
            return 
        for val in range(begin, end):
            if val in hashtable:
                for rid_idx, rid in enumerate(hashtable[val]):
                    ret_list.append(hashtable[val][rid_idx])
             
        return ret_list

    """
    # Create index/hashtable on specified column
    """
    def create_index(self, column_number):
        if self.indices[column_number] != None:
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
