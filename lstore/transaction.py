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
        self.log = {} # given a RID return record?
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
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            else:
                # result is a Record object
                self.records_modified.append(result)
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        # use log to update back to old value
        start_index = len(self.records_modified) - 1
        while start_index >= 0:
            query = self.queries[start_index][0]
            q = self.queries[start_index][2]
            if query == q.insert:
                self.undo_insert(q, self.records_modified.pop())

            elif query == q.delete:
                self.undo_delete(q, self.records_modified.pop())

            elif query == q.update:
                self.undo_update(q, self.records_modified.pop())

            start_index -= 1
        return False

    def commit(self):
        # release all locks
        start_index = len(self.records_modified) - 1
        while start_index >= 0:
            q = self.queries[start_index][2]
            table = q.table
            rid = self.records_modified.pop().rid 
            table.release_shared_lock(rid)
            table.release_exclusive_lock(rid)
            start_index -= 1
        
        return True

    # calls original delete()
    def undo_insert(self, query_undo, new_record):
        # Record in records_modified tells us RID we need to delete
        primary_key = new_record.columns[0]
        query_undo.delete(primary_key) 
        pass

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
            table.finish_page_access(page_location)
        pass

    def undo_update(self, query_undo, base_record):
        #create separate function to delete record
        RID = base_record.rid
        table = query_undo.table
        rec_addy = table.page_directory[RID]
        base_pr_id = rec_addy["page_range_id"]
        base_id = rec_addy["virtual_page_id"]
        base_id_int = base_id.split("_")[1]
        base_row = rec_addy["row"]

        base_page = self.page_ranges[base_pr_id].basePages[base_id_int]
        
        indir_page = table.access_page_from_memory(base_page.pages[INDIRECTION_COLUMN])
        indir_page.update(base_record.indirection, base_row)
        table.finish_page_access(base_page.pages[INDIRECTION_COLUMN])

        schema_page = table.access_page_from_memory(base_page.pages[SCHEMA_ENCODING_COLUMN])
        schema_page.update(base_record.schema_encoding, base_row)
        table.finish_page_access(base_page.pages[SCHEMA_ENCODING_COLUMN])
      
        pass