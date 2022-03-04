from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.records_modified = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.insert, grades_table, *records[key])
    """
    def add_query(self, query, table, *args):
        # query object we use to undo queries (their query object is "query" in self.queries)
        q = Query(table)
        self.queries.append((query, args, q))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args, q in self.queries:
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
        # TODO: commit to database
        # update the log
        return True

    
    def undo_insert(self, q, record):
        # Record in records_modified tells us RID we need to delete
        primary_key = record.columns[0]
        q.delete(primary_key)
        pass

    def undo_delete(self, q, record):
        RID = record.rid
        columns = q.table.log[RID]
        # TODO this will create a new record, might just want to change old record back instead
        q.update(record.columns[0], columns[5:])
        # create separate function to reinsert record (inplace update, changing indirection from -1)
        pass
    
    def undo_update(self, q, record):
        #create separate function to delete record
        pass   