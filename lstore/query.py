from lstore.table import Table, Record
from lstore.index import Index
import threading

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
MERGE_TRESH = 200 #TODO 

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    # Queries that fail must return False
    # Queries that succeed should return the result or True
    # Any query that crashes (due to exceptions) should return False
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
        try:
            RID = self.table.RID_directory[primary_key]
        except IndexError:
            return False
        row = self.table.page_directory[RID]["row"]
        page_range_id = self.table.page_directory[RID]["page_range_id"]
        page_range = self.table.page_ranges[page_range_id]
        virt_page_id = self.table.page_directory[RID]["virtual_page_id"]
        virt_index = page_range.get_ID_int(virt_page_id)
        # check bufferpool.page_ids_in_bufferpool
        cur_base_page = page_range.base_pages[virt_index]

        # TODO: change status in metadata columns. for now, only changing indirection column value so as to make sure merge is still fine
        for i in range(5,cur_base_page.num_columns):
            page = self.table.access_page_from_memory(cur_base_page.pages[i])
            # Update index
            self.table.index.delete_record(i-4, page.read(row), RID)
            if not page.delete(row):
                self.table.finish_page_access(cur_base_page.pages[i])
                return False
            self.table.finish_page_access(cur_base_page.pages[i])
        return True

    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        if columns == None:
            return False
        # Check if there is capacity or increase capacity if not
        self.increase_capacity_base()
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
        return True

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL or input(s) invalid
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, index_value, index_column, query_columns):
        if not self.table.index.has_index(index_column):
            print("cannot use index for selecting this column")
            return False

        rid_list = self.table.index.locate(index_column, index_value)
        if rid_list == False:
            return False
        if len(query_columns) != self.table.num_columns:
            return False

        rec_list = [] # contains rids of base pages (may need to go to tail pages if sche_enc == 1 for that col)


        for rid in rid_list:
            new_rec_cols = []
            # for every 1 in query columns
            for i, col in enumerate(query_columns):
                if col == 1: # user wants the data from that column
                    most_recent = self.get_most_recent_val(rid, i)
                    if most_recent != None:
                        new_rec_cols.append(most_recent[0])
                    else:
                        new_rec_cols.append(most_recent)
            new_rec = Record(0, 0, new_rec_cols, True) # (rid, key, columns, select bool)
            rec_list.append(new_rec)

        return rec_list

    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        self.increase_capacity_tail()
        tail_RID = self.table.create_new_RID()
        virtual_page = self.table.page_ranges[-1].tail_pages[-1]
        temp_page = self.table.access_page_from_memory(virtual_page.pages[0])
        tail_row = 8 * temp_page.get_num_records() # row we insert update at
        self.table.finish_page_access(virtual_page.pages[0])
        # Create record object
        cols = []
        updated_cols = []
        original_record_rid = self.table.RID_directory[primary_key]

        for i in range(0, self.table.num_columns):
            # get most recent record values
            if columns[i] == None:
                updated_cols.append(0)
            else:
                updated_cols.append(columns[i])
                # update index
                temp_tup = self.get_most_recent_val(original_record_rid, i)
                if temp_tup != None:
                    old_rid = temp_tup[1]
                    old_value = temp_tup[0]
                    self.table.index.update_record(i, old_value, columns[i], old_rid, tail_RID)
        record = Record(tail_RID, updated_cols[0], updated_cols, False, original_record_rid)
        #TODO: Do we pass in as *columns or columns?
        new_schema = self.create_new_schema(*columns)
        # Update record schema encoding
        if new_schema == 0:
            print("error: tail schema is 0")
        record.all_columns[SCHEMA_ENCODING_COLUMN] = new_schema
        record.schema_encoding = new_schema

        # indirection col
        ## get base record from page directory using primary key
        base_address = self.table.page_directory[original_record_rid]
        page_id = self.table.page_ranges[0].get_ID_int(base_address["virtual_page_id"])
        base_pr_id = base_address["page_range_id"]

        base_pr = self.table.page_ranges[base_pr_id]
        base_page = base_pr.base_pages[page_id]
        base_row = base_address["row"]

        # Getting indirection page of base page
        base_indirection_page_location = base_page.pages[INDIRECTION_COLUMN]
        base_indirection_page = self.table.access_page_from_memory(base_indirection_page_location)
        base_indirection = base_indirection_page.read(base_row) #getting the indirection of the base record
        base_indirection_page.update(tail_RID, base_row) # set base indirection as new tailrid
        base_indirection_page.dirty = True
        self.table.bufferpool.set_page_dirty(base_indirection_page)
        self.table.finish_page_access(base_indirection_page_location)

        # Set tail indirection to previous update (0 if there is none)
       
        record.all_columns[INDIRECTION_COLUMN] = base_indirection
        record.indirection = base_indirection
        
        # Getting & updating schema encoding of base page
        base_schema_page_location = base_page.pages[SCHEMA_ENCODING_COLUMN]
        schema_page = self.table.access_page_from_memory(base_schema_page_location)
        base_schema = schema_page.read(self.table.page_directory[original_record_rid]["row"])
        schema_page.update((base_schema | new_schema), base_row)
       
        schema_page.dirty = True
        self.table.bufferpool.set_page_dirty(schema_page)
        self.table.finish_page_access(base_schema_page_location)
        
        # Insert record into tail page
        #print(record.all_columns)
        #print("Inserting record")
        self.table.insert_record(virtual_page, record, tail_row)
        self.table.page_directory[tail_RID] = {
            "page_range_id" : self.table.page_ranges[-1].pr_id,
            "row" : tail_row,
            "virtual_page_id": self.table.page_ranges[-1].tail_page_id
        }
        ### MERGE SECTION ### 
        
        # base_page_old = self.table.page_ranges[base_address["page_range_id"]].base_pages[page_id]
        # base_page_old.num_updates += 1
        
        # # complete previous merge (consider changing new_copy_available to use a callback function instead)
        # if base_page_old.new_copy_available == True:
        #     # print("completing previous merge")
        #     # replace old bp WITH bp copy
        #     base_page_old = base_page_old.new_copy

        # # check if we need to merge (num_updates for curr base page)
        # if base_page_old.num_updates >= MERGE_TRESH:
        #     base_page_old.num_updates = 0
        #     # base_indirection_page_location = base_page_old.pages[INDIRECTION_COLUMN]
        #     # base_indirection_page = self.table.access_page_from_memory(base_indirection_page_location)
        #     # base_indirection = base_indirection_page.read(base_row) #getting the indirection of the base record
        #     thread = threading.Thread(target=self.table.merge, args=(base_page_old.copy(), base_indirection))
        #     thread.setDaemon(True)
        #     thread.start()

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
        if rid_list == [] or rid_list == None:
            return False
        sum = 0
        for rid in rid_list:
            val = self.get_most_recent_val(rid, aggregate_column_index)
            if val != None:
                sum += val[0]
        return sum


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
    increments one column of the record
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

    # given base page address, find the most recent value
    def get_most_recent_val(self, rid, column):
        # rid is not always base page
        rec_addy = self.table.page_directory[rid]
        # print(rec_addy)
        row = rec_addy["row"]
        id = rec_addy["virtual_page_id"]
        start_page = None
        isBase = False
        if id.split("_")[0] == "B":
            id_int = self.table.page_ranges[0].get_ID_int(id) 
            start_page = self.table.page_ranges[rec_addy["page_range_id"]].base_pages[id_int]
            isBase = True
        else:
            id_int = self.table.page_ranges[0].get_ID_int(id) 
            start_page = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[id_int] 

        schema_encoding_location = start_page.pages[SCHEMA_ENCODING_COLUMN]
        schema_encoding_page = self.table.access_page_from_memory(schema_encoding_location)
        sch_enc = bin(schema_encoding_page.read(row))[2:].zfill(self.table.num_columns)
        self.table.finish_page_access(schema_encoding_location)
        # if there is no update return bp, else search through tp
        if isBase and sch_enc[column] == '0':
            # TODO: Change from 4 to 5
            temp_page_location = start_page.pages[column+5]
            temp_page = self.table.access_page_from_memory(temp_page_location)
            data = temp_page.read(row)
            # print("col ", column, " data ", data)
            self.table.finish_page_access(temp_page_location)
            return (data, rid)
        else:
            indirection_location = start_page.pages[INDIRECTION_COLUMN]
            indirection_page = self.table.access_page_from_memory(indirection_location)
            tail_rid = indirection_page.read(row)
            self.table.finish_page_access(indirection_location)

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
                access_page = self.table.access_page_from_memory(tp.pages[column+5])
                data = access_page.read(row)
                self.table.finish_page_access(tp.pages[column+5])
                return (data, tail_rid)
            # else search through tail pages until we find it
            else:
                # TODO FIXME TODO FIXME ERROR IN HERE WITH GRABBING THE OLDEST TAIL PAGE VALUE, causing update error
                loop_count = 0
                prev_indir = tail_rid
                # rec_addy_tail2 = rec_addy_tail1
                while True:
                    if (loop_count >= 2):
                        print("count ", loop_count)
                        print(rec_addy_tail)
                        print(indir)
                    loop_count += 1
                    indirection_page = self.table.access_page_from_memory(tp.pages[INDIRECTION_COLUMN])
                    # this should be 710 not 0
                    indir = indirection_page.read(rec_addy_tail["row"])
                    self.table.finish_page_access(tp.pages[INDIRECTION_COLUMN])

                    if (indir == prev_indir):
                        print("error")
                        print(rec_addy_tail)
                        break
                    prev_indir = indir

                    rec_addy_tail = self.table.page_directory[indir]
                    tail_id = self.table.page_ranges[0].get_ID_int(rec_addy_tail["virtual_page_id"])
                    tail_row = rec_addy_tail["row"]

                    tp = self.table.page_ranges[rec_addy_tail["page_range_id"]].tail_pages[tail_id]
                    # check_tp_value
                    schema_page = self.table.access_page_from_memory(tp.pages[SCHEMA_ENCODING_COLUMN])
                    tail_schema = bin(schema_page.read(tail_row))[2:].zfill(self.table.num_columns)
                    self.table.finish_page_access(tp.pages[SCHEMA_ENCODING_COLUMN])
                    
                    if tail_schema[column] == '1':
                        # if value was found then add to list
                        access_page = self.table.access_page_from_memory(tp.pages[column+5])
                        data = access_page.read(tail_row)
                        self.table.finish_page_access(tp.pages[column+5])
                        return (data, indir)
                    else:
                        # error (should never reach end of TP without finding val)
                        if indir == 0:
                            # print("record not found in tail page")
                            break

    """
    # internal Method
    # make space if needed (assuming that we are getting a correct query
    # if physical page is full (hence base page is also full), add base page
    # returns False if invalid input, otherwise returns true
    """
    def increase_capacity_base(self):
        page_in_base_page = self.table.page_ranges[-1].base_pages[-1].pages[0]
        if not self.table.has_capacity_page(page_in_base_page):
            # if page range is full, add page range
            if not self.table.page_ranges[-1].has_capacity():
                self.table.create_new_page_range()
            else:
                pr_id = self.table.page_ranges[-1].pr_id
                self.table.add_base_page(pr_id)
        return True
    def increase_capacity_tail(self):
        page_in_base_page = self.table.page_ranges[-1].base_pages[-1].pages[0]
        if not self.table.has_capacity_page(page_in_base_page):
            # if not self.table.page_ranges[-1].base_pages[-1].pages[0].has_capacity():
            # if page range is full, add page range
            if not self.table.page_ranges[-1].has_capacity():
                self.table.create_new_page_range()
            else:
                pr_id = self.table.page_ranges[-1].pr_id
                self.table.add_base_page(pr_id)
        return True