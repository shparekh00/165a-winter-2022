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
    def delete(self, primary_key):
        RID = self.table.RID_directory[primary_key]
        row = self.table.page_directory[RID]["row"]
        page_range_id = self.table.page_directory[RID]["page_range_id"]
        page_range = self.table.page_ranges[page_range_id]
        virt_page_id = self.table.page_directory[RID]["virtual_page_id"]
        virt_index = page_range.get_ID_int(virt_page_id)
        
        # check bufferpool.page_ids_in_bufferpool
        cur_base_page = page_range.base_pages[virt_index]

        # TODO: change status in metadata columns. for now, only changing indirection column value so as to make sure merge is still fine
        # TODO: Change this to 5 when we add the BASE_RID metadata column
        for i in range(4,cur_base_page.num_columns):
            page = self.table.access_page_from_memory(cur_base_page.pages[i])
            # Update index
            self.table.index.delete_record(i-4, page.read(row), RID)
            if not page.delete(row):
                self.table.finish_page_access(cur_base_page.pages[i])
                return False
            self.table.finish_page_access(cur_base_page.pages[i])
        return True
   
    def increase_capacity(self, virtual_page_type):
        # make space if needed (assuming that we are getting a correct query
        # if physical page is full (hence base page is also full), add base page
        if virtual_page_type == "base":
            page_in_base_page = self.table.page_ranges[-1].base_pages[-1].pages[0]
            if not self.table.has_capacity_page(page_in_base_page):
            # if not self.table.page_ranges[-1].base_pages[-1].pages[0].has_capacity():
                # if page range is full, add page range
                if not self.table.page_ranges[-1].has_capacity():
                    self.table.create_new_page_range()
                else:
                    pr_id = self.table.page_ranges[-1].pr_id
                    self.table.add_base_page(pr_id)
        else:
            page_in_tail_page = self.table.page_ranges[-1].tail_pages[-1].pages[0]
            if not self.table.has_capacity_page(page_in_tail_page):
                if not self.table.page_ranges[-1].has_capacity():
                    self.table.create_new_page_range()
                else:
                    pr_id = self.table.page_ranges[-1].pr_id
                    self.table.add_tail_page(pr_id)
                
    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # Check if there is capacity 
        self.increase_capacity("base")
        
        # Create RID, get Page, get record row
        rid = self.table.create_new_RID()
        virtual_page_location = self.table.page_ranges[-1].base_pages[-1]

        page = None
        for pg_loc in self.table.page_ranges[-1].base_pages[-1].pages:
            page = self.table.access_page_from_memory(pg_loc)
            page.dirty = True
            self.table.bufferpool.set_page_dirty(page)
            self.table.finish_page_access(pg_loc)
        row = 8 * page.get_num_records()
        
        # Create record object
        record = Record(rid, columns[0], columns)

        # Insert into base page
        self.table.insert_record(virtual_page_location, record, row)

        # Insert RID in page directory (page range id, row, base_page_id)
        self.table.page_directory[rid] = {
            "page_range_id" : self.table.page_ranges[-1].pr_id,
            "row": row,
            "virtual_page_id": self.table.page_ranges[-1].base_page_id
        }

        # Map primary key to RID
        self.table.RID_directory[columns[self.table.key]] = rid

        # update index
        for i, val in enumerate(columns):
            self.table.index.insert_record(i, val, rid)
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
        if not self.table.index.has_index(index_column):
            print("cannot use index for selecting this column")

        rid_list = self.table.index.locate(index_column, index_value)
        rec_list = [] # contains rids of base pages (may need to go to tail pages if sche_enc == 1 for that col)
        # if rid_list != []:
        for rid in rid_list:
            new_rec_cols = []

            # for every 1 in query columns
            for i, col in enumerate(query_columns):
                if col == 1: # user wants the data from that column
                    new_rec_cols.append(self.get_most_recent_val(rid, i)[0])
            new_rec = Record(0, 0, new_rec_cols, True) # (rid, key, columns, select bool)
            rec_list.append(new_rec)

        return rec_list


    def create_new_schema(self, *columns):
        encoding_string = '' # used to OR with schema encoding to get new schema encoding
        for i in range(0, self.table.num_columns):
            if columns[i] == None:
                encoding_string += '0'
            else:
                encoding_string += '1'
        new_schema = int(encoding_string, 2)
        return new_schema

    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        self.increase_capacity("tail")

        tail_RID = self.table.create_new_RID()
        virtual_page = self.table.page_ranges[-1].tail_pages[-1]
        temp_page = self.table.access_page_from_memory(virtual_page.pages[0])
        row = 8 * temp_page.get_num_records()
        self.table.finish_page_access(virtual_page.pages[0])
        # Create record object
        cols = []
        updated_cols = []
        original_record_rid = self.table.RID_directory[primary_key]
        print("base_page_rid ", original_record_rid)
        for i in range(0, self.table.num_columns):
            # get most recent record values
            if columns[i] == None:
                updated_cols.append(0)
            else:
                updated_cols.append(columns[i])
                # update index
                # def update_record(self, old_value, value, old_rid, rid):
                temp_tup = self.get_most_recent_val(original_record_rid, i)
                #print("Tuple at iteration ", i, " is ", temp_tup)
                old_rid = temp_tup[1]
                print("old_rid ", old_rid)
                print("new rid ", tail_RID)
                old_value = temp_tup[0]
                print("last val in col ", i, " is ", old_value)
                print("new val: ", columns[i])
                self.table.index.update_record(i, old_value, columns[i], old_rid, tail_RID)
                
                
        record = Record(tail_RID, updated_cols[0], updated_cols)

        #TODO: Do we pass in as *columns or columns?
        new_schema = self.create_new_schema(*columns)
        print("new_schema: ", new_schema)
        # Update record schema encoding
        record.all_columns[SCHEMA_ENCODING_COLUMN] = new_schema
        record.schema_encoding = new_schema

        # indirection col
        ## get base record from page directory using primary key
        base_RID = self.table.RID_directory[primary_key]
        base_address = self.table.page_directory[base_RID]
        page_id = self.table.page_ranges[0].get_ID_int(base_address["virtual_page_id"])
        base_pr_id = base_address["page_range_id"]

        base_pr = self.table.page_ranges[base_pr_id]
        base_page = base_pr.base_pages[page_id]

        # Getting indirection page of base page
        indirection_page_location = base_page.pages[INDIRECTION_COLUMN]
        indirection_page = self.table.access_page_from_memory(indirection_page_location)
        base_indirection = indirection_page.read(row) #getting the indirection of the base record
        indirection_page.update(tail_RID, row)
        indirection_page.dirty = True
        self.table.bufferpool.set_page_dirty(indirection_page)
        self.table.finish_page_access(indirection_page_location)
        # Getting & updating schema encoding of base page
        base_schema_page_location = base_page.pages[SCHEMA_ENCODING_COLUMN]
        schema_page = self.table.access_page_from_memory(base_schema_page_location)
        base_schema = schema_page.read(self.table.page_directory[base_RID]["row"])
        schema_page.update((base_schema | new_schema), row)
        schema_page.dirty = True
        self.table.bufferpool.set_page_dirty(schema_page)
        self.table.finish_page_access(base_schema_page_location)
        # Set tail indirection to previous update (0 if there is none)
        record.all_columns[INDIRECTION_COLUMN] = base_indirection
        record.indirection = base_indirection
        # Insert record into tail page
        self.table.insert_record(virtual_page, record, row)
       
        self.table.page_directory[tail_RID] = {
            "page_range_id" : self.table.page_ranges[-1].pr_id,
            "row" : row,
            "virtual_page_id": self.table.page_ranges[-1].tail_page_id
        }
            
        pass

    # given bp addy, find the most recent value
    # TODO: return rid and value
    def get_most_recent_val(self, rid, column):

        rec_addy = self.table.page_directory[rid]
        row = rec_addy["row"]
        id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"]) 
        base_page = self.table.page_ranges[rec_addy["page_range_id"]].base_pages[id] 

        schema_encoding_location = base_page.pages[SCHEMA_ENCODING_COLUMN]
        schema_encoding_page = self.table.access_page_from_memory(schema_encoding_location)
        sch_enc = bin(schema_encoding_page.read(row))[2:].zfill(self.table.num_columns)
        self.table.finish_page_access(schema_encoding_location)
        # if there is no update return bp, else search through tp
        if sch_enc[column] == '0':
            # TODO: Change from 4 to 5
            temp_page_location = base_page.pages[column+4]
            temp_page = self.table.access_page_from_memory(temp_page_location)
            data = temp_page.read(row)
            self.table.finish_page_access(temp_page_location)
            #print("get most recent val () ...")
            #print("data: ", data)
            #print("indir: ", rid)
            return (data, rid)
        else:
            tail_rid_location = base_page.pages[INDIRECTION_COLUMN]
            tail_rid_page = self.table.access_page_from_memory(tail_rid_location)
            tail_rid = tail_rid_page.read(row)
            self.table.finish_page_access(tail_rid_location)

            rec_addy_tail = self.table.page_directory[tail_rid]
            id = self.table.page_ranges[0].get_ID_int(rec_addy_tail["virtual_page_id"])
            row = rec_addy_tail["row"]
            tp = self.table.page_ranges[rec_addy_tail["page_range_id"]].tail_pages[id]

            # TODO
            tail_sch_enc_location = tp.pages[SCHEMA_ENCODING_COLUMN]
            tail_sch_enc_page = self.table.access_page_from_memory(tail_sch_enc_location)
            tail_sch_enc = bin(tail_sch_enc_page.read(row))[2:].zfill(self.table.num_columns)
            self.table.finish_page_access(tail_sch_enc_location)
            # if first tail page is the one we want, return it
            if tail_sch_enc[column] == '1':
                # TODO: change from 4 to 5
                access_page = self.table.access_page_from_memory(tp.pages[column+4])
                data = access_page.read(row)
                self.table.finish_page_access(tp.pages[column+4])
                return (data, tail_rid)
            # else search through tail pages until we find it
            else:
                while True:
                    indirection_page = self.table.access_page_from_memory(tp.pages[INDIRECTION_COLUMN])
                    indir = indirection_page.read(rec_addy_tail["row"])
                    self.table.finish_page_access(tp.pages[INDIRECTION_COLUMN])

                    rec_addy_tail = self.table.page_directory[indir]
                    id = self.table.page_ranges[0].get_ID_int(rec_addy_tail["virtual_page_id"])
                    tp = self.table.page_ranges[rec_addy_tail["page_range_id"]].tail_pages[id]
                    row = rec_addy_tail["row"]
                    # check_tp_value

                    schema_page = self.table.access_page_from_memory(tp.pages[SCHEMA_ENCODING_COLUMN])
                    tail_sch_enc = bin(schema_page.read(row))[2:].zfill(self.table.num_columns)
                    self.table.finish_page_access(tp.pages[SCHEMA_ENCODING_COLUMN])

                    if tail_sch_enc[column] == '1':
                        # if value was found then add to list
                        access_page = self.table.access_page_from_memory(tp.pages[column+4])
                        data = access_page.read(row)
                        self.table.finish_page_access(tp.pages[column+4])
                        return (data, indir)
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
            sum += self.get_most_recent_val(rid, aggregate_column_index)[0]
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
