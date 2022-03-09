from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.records_modified = []
        self.lock_manager = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    """
    # t.add_query(q.insert, grades_table, *records[key]) 
    def add_query(self, query_function, table, *args):
        # query object we use to undo queries (their query object is "query" in self.queries)
        query_undo = Query(table) # we create a query object to undo their queries
        self.queries.append((query_function, args, query_undo))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args, query_undo in self.queries:
            # result contains record of query for database altering functions (insert, update, delete)
            if query.__name__ == 'insert':
                vp_id_lock = self.get_insert_lock(query_undo)
                if vp_id_lock == False:
                    print("aborting")
                    return self.abort()
                else:
                    self.lock_manager.append(vp_id_lock)

            result = query(*args)
            # If the query has failed the transaction should abort
            
            if result == False:
                print("aborting")
                return self.abort()
            elif result.all_columns == "":
                print("why are you none??")
            else:
                # result is a Record object
                #print("result is: ", result.all_columns)
                
                self.records_modified.append(result)
                print("records modified ", len(self.records_modified))
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        # use log to update back to old value
        start_index = len(self.records_modified) - 1
        while start_index >= 0:
            query = self.queries[start_index][0]
            q = self.queries[start_index][2]
            if query.__name__ == 'insert':
                self.undo_insert(q, self.records_modified[-1])

            elif query.__name__ == "delete":
                self.undo_delete(q, self.records_modified[-1])

            elif query.__name__ == 'update':
                self.undo_update(q, self.records_modified[-1])

            self.release_locks(start_index)
            start_index -= 1
        return False

    def commit(self):
        # release all locks
        start_index = len(self.records_modified) - 1

        while start_index >= 0:
            #print(self.queries)
            self.release_locks(start_index)
            start_index = len(self.records_modified) - 1

        print("Finished committing")
        return True


    # calls original delete()
    def undo_insert(self, query_undo, new_record):
        # Record in records_modified tells us RID we need to delete
        primary_key = new_record.columns[0]
        query_undo.delete_insert(primary_key)
        table = query_undo.table
        # delete_record(self, column, value, rid)
        for col, val in enumerate(new_record.columns):
            table.index.delete_record(col, val, new_record.rid)




    # iterate through columsn of "deleted" record by inserting with original metadata (same RID, indir...)
    def undo_delete(self, query_undo, old_record):
        RID = old_record.rid
        table = query_undo.table
        rec_addy = table.page_directory[RID]
        pr_id = rec_addy["page_range_id"]
        vp_id = rec_addy["virtual_page_id"]
        vp_id_int = vp_id.split("_")[1]
        row = rec_addy["row"]

        # posssibly mark pages as dirty?
        # iterate through each column, and replace all values with old record
        for i, val in enumerate(old_record.all_columns):
            page_location = table.page_ranges[pr_id].base_pages[vp_id_int].pages[i]
            page = table.access_page_from_memory(page_location)
            page.update(val, row)
            table.bufferpool.set_page_dirty(page)
            table.finish_page_access(page_location)

            table.index.insert_record(i, val, old_record.rid)



    def undo_update(self, query_undo, base_record): # base_record is the original record before performing update
        #create separate function to delete record
        RID = base_record.rid
        table = query_undo.table
        rec_addy = table.page_directory[RID]
        base_pr_id = rec_addy["page_range_id"]
        base_id = rec_addy["virtual_page_id"]
        base_id_int = base_id.split("_")[1]
        base_row = rec_addy["row"]

        base_page = table.page_ranges[base_pr_id].base_pages[int(base_id_int)]

        indir_page = table.access_page_from_memory(base_page.pages[INDIRECTION_COLUMN])
        # read indirection page at base row before we change it to get tail_rid for update
        tail_RID = indir_page.read(base_row)
        tail_rec_addy = table.page_directory[tail_RID]
        tail_pr_id = tail_rec_addy["page_range_id"]
        tail_id = tail_rec_addy["virtual_page_id"]
        tail_id_int = tail_id.split("_")[1]
        tail_row = tail_rec_addy["row"]
        tail_page = table.page_ranges[tail_pr_id].tail_pages[int(tail_id_int)]

        # Get tail schema to know which column this tail record updated
        tail_schema_page = table.access_page_from_memory(tail_page.pages[SCHEMA_ENCODING_COLUMN])
        tail_schema = tail_schema_page.read(tail_row)
        table.finish_page_access(tail_page.pages[SCHEMA_ENCODING_COLUMN])
        schema = bin(tail_schema)[2:].zfill(self.table.num_columns)
        for i in range(0, table.num_columns):
            if schema[i] == '1':
                updated_column = i + 5
                updated_column_page = table.access_pages_from_memory(tail_page.pages[i+5])
                updated_value = updated_column_page.read(tail_row)
                table.finish_page_access(tail_page.pages[i+5])
                break

        indir_page.update(base_record.indirection, base_row)
        table.finish_page_access(base_page.pages[INDIRECTION_COLUMN])

        schema_page = table.access_page_from_memory(base_page.pages[SCHEMA_ENCODING_COLUMN])
        schema_page.update(base_record.schema_encoding, base_row)
        table.bufferpool.set_page_dirty(schema_page)
        table.finish_page_access(base_page.pages[SCHEMA_ENCODING_COLUMN])

        bin(tail_schema)[2:].zfill(self.table.num_columns)
        # TODO delete record from index for all data columns (look at how it was done in query.delete)
        # update_record(self, column, old_value, value, old_rid, rid):
        table.index.update_record(updated_column, updated_value, base_record.all_columns[updated_column], tail_RID, RID)

    def release_locks(self, start_index):
        print("self.queries length: ", len(self.queries))
        print("start index: ", start_index)

        q = self.queries[start_index][2]
        table = q.table

        # Extract the virtual page id
        rid = self.records_modified.pop().rid
        vp_id = table.page_directory[rid]["virtual_page_id"]

        # Release locks
        table.release_shared_lock(vp_id)
        table.release_exclusive_lock(vp_id)
        self.lock_manager.clear()

    def get_insert_lock(self, query_undo):
        # Check if there is capacity or increase capacity if not
        # first lock virtual page, then check if we need to lock a diff page after increasing capacity
        vp_id = query_undo.table.page_ranges[-1].base_pages[-1].page_id
        if vp_id in self.lock_manager:
            return True
        got_lock = query_undo.table.get_exclusive_lock(vp_id)
        if query_undo.increase_capacity_base() == True:
            print("created space for insert")
            # update lock to new base page and release original lock
            query_undo.table.release_exclusive_lock(vp_id)
            if query_undo.table.get_exclusive_lock(query_undo.table.page_ranges[-1].base_pages[-1].page_id) == False:
                print("failed to get lock in insert after increasing capacity")
                return False
        # if space was not created and failed to get lock originally, return false
        else:
            if not got_lock:
                print("Failed to get lock in insert without increasing capacity")
                return False
        return vp_id
