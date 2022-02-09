from lstore.table import Table, Record
from lstore.index import Index

INDIRECTION_COLUMN = 0
RID_COLUMN = 1 
TIMESTAMP_COLUMN = 2 
SCHEMA_ENCODING_COLUMN = 3

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
    # Read a record with specified RIDw
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    #assuming primary_key here means RID
    #delete record with SID 916572884
    def delete(self, primary_key):
        RID = self.table.RID_directory[primary_key] 
        address  = self.table.page_directory[RID] 
        virtualPageId = self.table.page_ranges[0].get_ID_int(address["virtual_page_id"])
        # self.table.page_ranges[0].get_ID_int(base_address["virtual_page_id"])
        cur_base_page = self.table.page_ranges[address["page_range_id"]].base_pages[virtualPageId]
        
        # change status in metadata columns. for now, only changing indirection column value so as to make sure merge is still fine
        row = address["row"]
        cur_base_page.pages[0].write(-1, row) # -1 as RIDs are all positive so we can flag these as deleted
        for i in range(4,cur_base_page.num_columns-4):
            if not cur_base_page.pages[i].delete(row):
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
        # num columns * 8 * num records should be location in bytearray
        location = 8 * self.table.page_ranges[-1].base_pages[-1].pages[0].get_num_records()
        
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
    #query columns = [0,0,0,1,1]
    def select(self, index_value, index_column, query_columns):
        #locate(self, column, value)
        rid_list = self.table.index.locate(index_column, index_value)
        # if rid_list != []:
        #     print(rid_list)
        rec_list = [] # contains rids of base pages (may need to go to tail pages if sche_enc == 1 for that col)
        for rid in rid_list:
            rec_addy = self.table.page_directory[rid]
            new_rec_cols = []
            # rec_addy["virtual_page_id"]: "B_1" 1
            # addy_s = rec_addy["virtual_page_id"].split("_")
            # addy_s = addy_s[-1]

            # Base page address
            id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
            row = rec_addy["row"]
            base_page = self.table.page_ranges[rec_addy["page_range_id"]].base_pages[id]
            # check schema encoding
            sch_enc = bin(base_page.pages[SCHEMA_ENCODING_COLUMN].read(row))[2:].zfill(self.table.num_columns-4)
            # 00000
            # 00100 -> 4
            # 00010 -> 2
            # 00001 -> 1
            # if int(bin(sch_enc)) != 0 :
            
            # TODO TODO TODO TODO TODO TODO TODO TODO 
            # for every 1 in query columns, 
            # if sch_enc at that column is 1, search in tp, else return bp
            for i, col in enumerate(query_columns):
                if col == 1:
                    if sch_enc[i] == '1':
                        tail_rid = self.table.page_ranges[rec_addy["page_range_id"]].base_pages[id].pages[INDIRECTION_COLUMN].read(row)
                        rec_addy = self.table.page_directory[tail_rid]
                        id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
                        row = rec_addy["row"]
                        tail_sch_enc = bin(self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id].pages[SCHEMA_ENCODING_COLUMN].read(row))[2:].zfill(self.table.num_columns-4)
                        #
                        if tail_sch_enc[i] == '1':
                            new_rec_cols.append(self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id].pages[i+4].read(row))
                        else:
                            indir = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id].pages[INDIRECTION_COLUMN].read(rec_addy["row"])
                            while indir != 0:
                                rec_addy = self.table.page_directory[indir]
                                id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
                                tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id]
                                row = rec_addy["row"]
                                indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])
                                # check_tp_value
                                tail_sch_enc = bin(self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id].pages[SCHEMA_ENCODING_COLUMN].read(row))[2:].zfill(self.table.num_columns-4)
                                if tail_sch_enc == '1':
                                    # if value was found then add to list
                                    new_rec_cols.append(self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id].pages[i+4].read(row))

                        #print("tail page", self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id].pages[i+4].read(row))
                    else:
                        rec_addy = self.table.page_directory[rid]
                        id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
                        row = rec_addy["row"]
                        new_rec_cols.append(self.table.page_ranges[rec_addy["page_range_id"]].base_pages[id].pages[i+4].read(row))
                        #print("base page", self.table.page_ranges[rec_addy["page_range_id"]].base_pages[id].pages[i+4].read(row))
            #print(new_rec_cols)
            #     new_rec_cols.append(col)
            #     #if query_columns[col] == 1
            #     #add column to record
            # #add record to ret_cols
            # #__init__(self, rid, key, columns, select=False)
            # key = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id].pages[self.table.key]
            # new_rec = Record(0, key, new_rec_cols, True)
            # rec_list.append(new_rec)
                    
            
            # if sch_enc != '0' : 
            #     # look up tail page with indirection column
            #     indir_RID = base_page.pages[INDIRECTION_COLUMN].read(row)
            #     rec_addy = self.table.page_directory[indir_RID]
            #     id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
                # TODO: Get most recent value from tail page if schema encoding from tail page matches query_columns, otherwise grab from value from base page


            #tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[rec_addy["virtual_page_id"]]
            #row = rec_addy["row"]
            #print(id)
            # apend only columns users wants

            
            # File "c:\Users\cpire\Desktop\Misc\UC Davis\2022\Winter 2022\ECS 165A\Project\165a-winter-2022\lstore\query.py", line 107, in select
            # for i, col in enumerate(self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id].pages): #len 9
            # IndexError: list index out of range
            # try:
            #     x = self.table.page_ranges[rec_addy["page_range_id"]]
            #     y = x.tail_pages[2] #id
            #     z = y.pages
        
            # except Exception as e:
            #     print(len(x.tail_pages))
            #     print(id)
            #     print("AAAAAAAAHHHHHHHHHH")
            
            """
            for i, col in enumerate(self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id].pages): #len 9
                if i < 4 and query_columns[i-4] == 0:
                    continue
                if query_columns[i-4] == 0: # len 5
                    continue
            """
                
            
            #TODO get rid of first 4 columns (idxn, sch enc, timestamp) --> DONE: Added extra param to Record class.
            #getting rid's from locate function
            #need to now add the columns requested by query_columns
        return rec_list
        pass
        #so basically rn we dont know how to make the record instances

    # given a RID and query_columns, returns a record object with the specified columns
    def get_record_from_RID(self, RID, query_columns):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):

        # if physical page is full (hence tail page is also full), add tail page
        if not self.table.page_ranges[-1].tail_pages[-1].pages[0].has_capacity():
            # if page range is full, add page range
            if not self.table.page_ranges[-1].has_capacity():
                self.table.create_new_page_range()
            self.table.page_ranges[-1].add_tail_page()

        location = 8 * self.table.page_ranges[-1].tail_pages[-1].pages[0].get_num_records()
        #print("location: ", location)

        tail_RID = self.table.create_new_RID()
            
        #schema_encoding = '0' * self.table.num_columns
        # create record object
        record = Record(tail_RID, columns[0], columns)
        
        # schema encoding (equal to col that contains updated va) (set null values to 0)
        encoding_string = '' # used to OR with schema encoding to get new schema encoding
        for i in range(4, self.table.num_columns):
            #print("col value: " , columns[i-4])
            if not columns[i-4]: 
                record.all_columns[i] = 0
                encoding_string += '0'
            else: 
                #record.schema_encoding[i-4] = True
                encoding_string += '1'
       
        #record.all_columns[3] = int(record.all_columns[3] | int(encoding_string, 2)) # base record
        new_schema = int(encoding_string, 2)
        #print("schema " , new_schema)
        record.all_columns[3] = new_schema # tail record schema encoding

        # indirection col
        ## get base record from page directory using primary key
        base_RID = self.table.RID_directory[primary_key]
        base_address = self.table.page_directory[base_RID]
        #print("RID: ", base_RID)
        #print("address ", base_address)
        page_id = self.table.page_ranges[0].get_ID_int(base_address["virtual_page_id"])
        #print("Page id: ", page_id) #FIXME
        base_indirection = self.table.page_ranges[base_address["page_range_id"]].base_pages[page_id].pages[0] #getting the indirection of the base record
        base_schema_page = self.table.page_ranges[base_address["page_range_id"]].base_pages[page_id].pages[SCHEMA_ENCODING_COLUMN]
        row = self.table.page_directory[base_RID]["row"]    
        base_schema = base_schema_page.read(self.table.page_directory[base_RID]["row"])

        self.table.page_ranges[base_address["page_range_id"]].base_pages[page_id].pages[SCHEMA_ENCODING_COLUMN].update((base_schema | new_schema), row)
        
        # set tail indirection to previous update (0 if there is none)
        record.indirection = base_indirection
        ## update base page record's indirection column with tail page's new RID
        #base_indirection = tail_RID
        #print("before: ", tail_RID)
        self.table.page_ranges[base_address["page_range_id"]].base_pages[page_id].pages[INDIRECTION_COLUMN].update(tail_RID, row)
        #print("after: ", self.table.page_ranges[base_address["page_range_id"]].base_pages[page_id].pages[INDIRECTION_COLUMN].read(row))
                
        # insert record
        #print(record.all_columns)
        self.table.page_ranges[-1].tail_pages[-1].insert_record(record, location)
        #for i in range(4, 9)
            #print("column ", i, ": ", self.table.page_ranges[-1].tail_pages[-1].pages[i].read(location))
        
        

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
