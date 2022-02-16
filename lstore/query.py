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
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):

        # make space if needed (assuming that we are getting a correct query
        # if physical page is full (hence base page is also full), add base page
        if not self.table.page_ranges[-1].base_pages[-1].pages[0].has_capacity():
            # if page range is full, add page range
            if not self.table.page_ranges[-1].has_capacity():
                self.table.create_new_page_range()

            id_pr = self.table.page_ranges[-1].pr_id
            self.table.page_ranges[-1].add_base_page(id_pr)
        # create RID
        # num columns * 8 * num records should be location in bytearray
        location = 8 * self.table.page_ranges[-1].base_pages[-1].pages[0].get_num_records()
        #rid = columns[self.table.key]
        rid = self.table.create_new_RID()
        # create record object
        record = Record(rid, columns[0], columns)
        #print("inserting ", columns[0])
        # insert into base page
        self.table.page_ranges[-1].base_pages[-1].insert_record(record, location)
        # insert RID in page directory (page range id, row, )
        self.table.page_directory[rid] = {
            "page_range_id" : self.table.page_ranges[-1].pr_id,
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
        #print("searching for: ", index_value)
        rid_list = self.table.index.locate(index_column, index_value)
        rec_list = [] # contains rids of base pages (may need to go to tail pages if sche_enc == 1 for that col)
        # if rid_list != []:
        for rid in rid_list:
            new_rec_cols = []

            # for every 1 in query columns
            for i, col in enumerate(query_columns):
                if col == 1: # user wants the data from that column
                    new_rec_cols.append(self.get_most_recent_val(rid, i))
            new_rec = Record(0, 0, new_rec_cols, True) # (rid, key, columns, select bool)
            rec_list.append(new_rec)

        return rec_list

    # given a RID and query_columns, returns a record object with the specified columns
    def get_record_from_RID(self, RID, query_columns):
        pass


    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):

        # if physical page is full (hence tail page is also full), add tail page
        if not self.table.page_ranges[-1].tail_pages[-1].pages[0].has_capacity():
            # if page range is full, add page range
            if not self.table.page_ranges[-1].has_capacity():
                self.table.create_new_page_range()
            id_pr = self.table.page_ranges[-1].pr_id
            self.table.page_ranges[-1].add_tail_page(id_pr)

        location = 8 * self.table.page_ranges[-1].tail_pages[-1].pages[0].get_num_records()

        tail_RID = self.table.create_new_RID()

        # create record object
        cols = []
        updated_cols = []
        original_record_rid = self.table.RID_directory[primary_key]
        for i in range(0, self.table.num_columns):
            # get most recent record values
            if columns[i] == None:
                updated_cols.append(0)
                #updated_cols.append(self.get_most_recent_val(original_record_rid, i))
            else:
                updated_cols.append(columns[i])
        record = Record(tail_RID, updated_cols[0], updated_cols)


        # schema encoding (equal to col that contains updated va) (set null values to 0)
        encoding_string = '' # used to OR with schema encoding to get new schema encoding
        for i in range(0, self.table.num_columns):
            if columns[i] == None:
                encoding_string += '0'
            else:
                encoding_string += '1'

        new_schema = int(encoding_string, 2)
        record.all_columns[3] = new_schema #-- which way is correct? lol
        record.schema_encoding = new_schema # tail record schema encoding

        # indirection col
        ## get base record from page directory using primary key
        base_RID = self.table.RID_directory[primary_key]
        base_address = self.table.page_directory[base_RID]

        page_id = self.table.page_ranges[0].get_ID_int(base_address["virtual_page_id"])
        #print("Page id: ", page_id) #FIXME
        row = self.table.page_directory[base_RID]["row"]
        base_indirection = self.table.page_ranges[base_address["page_range_id"]].base_pages[page_id].pages[INDIRECTION_COLUMN].read(row) #getting the indirection of the base record
        base_schema_page = self.table.page_ranges[base_address["page_range_id"]].base_pages[page_id].pages[SCHEMA_ENCODING_COLUMN]
        base_schema = base_schema_page.read(self.table.page_directory[base_RID]["row"])

        self.table.page_ranges[base_address["page_range_id"]].base_pages[page_id].pages[SCHEMA_ENCODING_COLUMN].update((base_schema | new_schema), row)

        # set tail indirection to previous update (0 if there is none)
        record.all_columns[INDIRECTION_COLUMN] = base_indirection
        ## update base page record's indirection column with tail page's new RID
        self.table.page_ranges[base_address["page_range_id"]].base_pages[page_id].pages[INDIRECTION_COLUMN].update(tail_RID, row)

        # insert record
        #print(record.all_columns)
        #print(record.all_columns)
        self.table.page_ranges[-1].tail_pages[-1].insert_record(record, location)
        #for i in range(4, 9)
            #print("column ", i, ": ", self.table.page_ranges[-1].tail_pages[-1].pages[i].read(location))

        self.table.page_directory[tail_RID] = {
            "page_range_id" : self.table.page_ranges[-1].pr_id,
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

    # given bp addy, find the most recent value
    def get_most_recent_val(self, rid, column):
        rec_addy = self.table.page_directory[rid]
        row = rec_addy["row"]
        id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"]) 
        base_page = self.table.page_ranges[rec_addy["page_range_id"]].base_pages[id] 
        sch_enc = bin(base_page.pages[SCHEMA_ENCODING_COLUMN].read(row))[2:].zfill(self.table.num_columns)

        # if there is no update return bp, else search through tp
        if sch_enc[column] == '0':
            return base_page.pages[column+4].read(row)
        else:
            tail_rid = base_page.pages[INDIRECTION_COLUMN].read(row)
            rec_addy_tail = self.table.page_directory[tail_rid]
            id = self.table.page_ranges[0].get_ID_int(rec_addy_tail["virtual_page_id"])
            row = rec_addy_tail["row"]
            tp = self.table.page_ranges[rec_addy_tail["page_range_id"]].tail_pages[id]
            tail_sch_enc = bin(tp.pages[SCHEMA_ENCODING_COLUMN].read(row))[2:].zfill(self.table.num_columns)
            # if first tail page is the one we want, return it
            if tail_sch_enc[column] == '1':
                return tp.pages[column+4].read(row)
            # else search through tail pages until we find it
            else:
                while True:
                    indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy_tail["row"])
                    rec_addy_tail = self.table.page_directory[indir]
                    id = self.table.page_ranges[0].get_ID_int(rec_addy_tail["virtual_page_id"])
                    tp = self.table.page_ranges[rec_addy_tail["page_range_id"]].tail_pages[id]
                    row = rec_addy_tail["row"]
                    # check_tp_value

                    #print(bin(tp.pages[SCHEMA_ENCODING_COLUMN].read(row))[2:].zfill(self.table.num_columns))
                    tail_sch_enc = bin(tp.pages[SCHEMA_ENCODING_COLUMN].read(row))[2:].zfill(self.table.num_columns)
                    if tail_sch_enc[column] == '1':
                        # if value was found then add to list
                        return tp.pages[column+4].read(row)
                    else:
                        # error (should never reach end of TP without finding val)
                        if indir == 0:
                            print("record not found in tail page")
                            break


    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        rid_list = self.table.index.locate_range(start_range, end_range, aggregate_column_index)
        
        if rid_list == []:
            print("rid list empty")
            return False
        sum = 0
        for rid in rid_list:
            sum += self.get_most_recent_val(rid, aggregate_column_index)
        return sum

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
