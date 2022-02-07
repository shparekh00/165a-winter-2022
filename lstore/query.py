from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    #assuming primary_key here means RID
    #delete record with SID 916572884
    def delete(self, primary_key):
        RID = primary_key
        address  = self.table.page_directory[RID]
        cur_page_range = self.table.page_ranges[address.page_range_id]
        cur_base_page = cur_page_range.base_pages[address.virtual_page_id]
        # change status in metadata columns. for now, only changing RID column value so as to make sure merge is still fine
        cur_base_page.pages[1].write(-1) # -1 as RIDs are all positive so we can flag these as deleted
        for i in range(4,cur_base_page.num_columns-4):
            if not cur_base_page.pages[i].delete(address.row):
                return False
        pass
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):

        # make space if needed (assuming that we are getting a correct query
        
        # if physical page is full (hence base page is also full), add base page
        if not self.table.page_ranges[-1].base_pages[-1].pages[0].has_capacity():
            # if page range is full, add page range
            if not self.table.page_ranges[-1].has_capacity():
                self.table.create_new_page_range()
            self.table.page_ranges[-1].add_base_page()


        # create RID
        # num columns * 4 * num records should be location in bytearray
        location = 4 * self.table.page_ranges[-1].base_pages[-1].pages[0].get_num_records()
        #rid = str(self.table.page_range_id) + "_" + str(self.table.page_ranges[-1].base_pages[-1].page_id) + "_" + str(location)
        
        #rid = columns[self.table.key]
        rid = self.table.create_new_RID()

        
        
        # create record object
        record = Record(rid, columns[0], columns)
        # insert into base page
        self.table.page_ranges[-1].base_pages[-1].insert_record(record)
        # insert RID in page directory (page range id, row, )
        self.table.page_directory[rid] = {
            "page_range_id" : self.table.page_range_id,
            "row" : location,
            "virtual_page_id": self.table.page_ranges[-1].base_page_id
        }
        self.table.RID_directory[columns[self.table.key]] = rid
        #TODO: ADD PRIMARY KEY (196572883) AS A KEY IN DICT
        
        
        
        pass

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_value, index_column, query_columns):
        pass
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        # primary key is first column in the record
        # look up page range from page directory in table
        # append to active tail page in page range (the last one in the array of tail pages)
        # figure out how to append to correct column in tail page

        # if physical page is full (hence tail page is also full), add tail page
        if not self.table.page_ranges[-1].tail_pages[-1].pages[0].has_capacity():
            # if page range is full, add page range
            if not self.table.page_ranges[-1].has_capacity():
                self.table.create_new_page_range()
            self.table.page_ranges[-1].add_tail_page()


        location = 4 * self.table.page_ranges[-1].tail_pages[-1].pages[0].get_num_records()

        tail_RID = self.table.create_new_RID()
            
        #schema_encoding = '0' * self.table.num_columns
        # create record object
        record = Record(tail_RID, columns[0], columns)
        
        # schema encoding (equal to col that contains updated va) (set null values to 0)
        for i in range(4, self.table.num_columns):
            if not columns[i-4]: 
                record.all_columns[i] = 0
            else: 
                record.schema_encoding = i

        # indirection col
        ## get base record from page directory using primary key
        base_RID = self.table.RID_directory[primary_key]
        base_address = self.table.page_directory[base_RID]
    
        page_id = self.table.page_ranges[0].get_ID_int(base_address["virtual_page_id"])
        print(page_id) #FIXME
        temp = self.table.page_ranges[base_address["page_range_id"]]
        # TODO fix bug
        # Exception has occurred: IndexError
        # list index out of range
        temp2 = temp.base_pages[page_id]
        temp3 = temp2.pages[0]
        base_indirection = temp3 #lol
         
        # set tail indirection to previous update (0 if there is none)
        record.indirection = base_indirection
        ## update base page record's indirection column with tail page's new RID
        base_indirection = tail_RID
                
        # insert record
        self.table.page_ranges[-1].tail_pages[-1].insert_record(record)

        self.table.page_directory[tail_RID] = {
            "page_range_id" : self.table.page_range_id,
            "row" : location,
            "virtual_page_id": self.table.page_ranges[-1].tail_page_id
        }


        # Make new RID for tail pages (we need to figure out how to implement this)
        # Get old record from page directory using primary key
        # Update old base page record's indirection column with tail page's new RID
        # (That way to get to the latest tail page we use the primary key to get the base page
        # and then go its indirection column to get the RID of the latest tail page)
        # and then we can index the page directory with that new RID

        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
