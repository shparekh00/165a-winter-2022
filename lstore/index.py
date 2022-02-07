"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

INDIRECTION_COLUMN = 0
RID_COLUMN = 1 
TIMESTAMP_COLUMN = 2 
SCHEMA_ENCODING_COLUMN = 3

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = [None] *  self.table.num_columns
        
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        #return list of RIDs that match values?
        #1. go to correct column in base
        #2. go through column to find records with the given value
        #3. check schema encoding column to see if record was modified (check for a 1)
        #3a. schema enc 0 AND we find value, note RID into list
        #3b. schema enc 1: search tp for matches. go back to bp to go through unupdated values
        #4. return list
        ret_list = []
        
        # go to each page range and repeat process until we run out of page ranges
        
        for pr in range(0, len(self.table.page_ranges)):
            for bp in range(0, len(self.table.page_ranges[pr].base_pages)):
                col_page = self.table.page_ranges[pr].base_pages[bp][column]
                for base_row in range(0, self.table.page_ranges[pr].base_pages[bp].get_num_records()):
                    sch_enc = self.table.page_ranges[pr].base_pages[bp][SCHEMA_ENCODING_COLUMN][base_row]
                    # if record was not updated and value matches, add it to ret_list
                    if sch_enc == 0:
                        if self.table.page_ranges[pr].base_pages[bp][column][base_row] == value:
                            ret_list.append(self.table.page_ranges[pr].base_pages[bp][RID_COLUMN][base_row])
                        continue
                    # otherwise if value doesn't match, but the record was updated, check tail page for a match
                    elif sch_enc == 1:
                        
                        tail_rid = self.table.page_ranges[pr].base_pages[bp][INDIRECTION_COLUMN][base_row]
                        rec_addy = self.table.page_directory[tail_rid]
                        tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[rec_addy["virtual_page_id"]]
                        # if tail record contains updated column AND we found the value
                        if (tp[SCHEMA_ENCODING_COLUMN][rec_addy["row"]] == column) and (tp[column][rec_addy["row"]] == value):
                            # if value was found then add to list
                            ret_list.append(self.table.page_ranges[pr].base_pages[bp][RID_COLUMN][base_row])
                            continue
                        # otherwise check any remaining tail pages 
                        else:   
                            indir = tp[INDIRECTION_COLUMN][rec_addy["row"]]
                            # otherwise go to indirection column and check if another tail_RID exists, if so go to if and repeat check
                            while indir != 0:
                                rec_addy = self.table.page_directory[indir]
                                tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[rec_addy["virtual_page_id"]]
                                indir = tp[INDIRECTION_COLUMN][rec_addy["row"]]
                                # check_tp_value
                                if (tp[SCHEMA_ENCODING_COLUMN][rec_addy["row"]] == column) and (tp[column][rec_addy["row"]] == value):
                                    # if value was found then add to list
                                    ret_list.append(self.table.page_ranges[pr].base_pages[bp][RID_COLUMN][base_row])
                                    break
    
        #if nothing matches, ret_list will be empty       
        return ret_list

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    #we did inclusive between
    def locate_range(self, begin, end, column): 
        ret_list = []
        
        # go to each page range and repeat process until we run out of page ranges
        
        for pr in range(0, len(self.table.page_ranges)):
            for bp in range(0, len(self.table.page_ranges[pr].base_pages)):
                col_page = self.table.page_ranges[pr].base_pages[bp][column]
                for base_row in range(0, self.table.page_ranges[pr].base_pages[bp].get_num_records()):
                    sch_enc = self.table.page_ranges[pr].base_pages[bp][SCHEMA_ENCODING_COLUMN][base_row]
                    # if record was not updated and value matches, add it to ret_list
                    if sch_enc == 0:
                        if self.table.page_ranges[pr].base_pages[bp][column][base_row] >= begin and self.table.page_ranges[pr].base_pages[bp][column][base_row] <= end:
                            ret_list.append(self.table.page_ranges[pr].base_pages[bp][RID_COLUMN][base_row])
                        continue
                    # otherwise if value doesn't match, but the record was updated, check tail page for a match
                    elif sch_enc == 1:
                        
                        tail_rid = self.table.page_ranges[pr].base_pages[bp][INDIRECTION_COLUMN][base_row]
                        rec_addy = self.table.page_directory[tail_rid]
                        tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[rec_addy["virtual_page_id"]]
                        # if tail record contains updated column AND we found the value
                        if (tp[SCHEMA_ENCODING_COLUMN][rec_addy["row"]] == column) and (tp[column][rec_addy["row"]] >= begin and tp[column][rec_addy["row"]] <= end):
                            # if value was found then add to list
                            ret_list.append(self.table.page_ranges[pr].base_pages[bp][RID_COLUMN][base_row])
                            continue
                        # otherwise check any remaining tail pages 
                        else:   
                            indir = tp[INDIRECTION_COLUMN][rec_addy["row"]]
                            # otherwise go to indirection column and check if another tail_RID exists, if so go to if and repeat check
                            while indir != 0:
                                rec_addy = self.table.page_directory[indir]
                                tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[rec_addy["virtual_page_id"]]
                                indir = tp[INDIRECTION_COLUMN][rec_addy["row"]]
                                # check_tp_value
                                if (tp[SCHEMA_ENCODING_COLUMN][rec_addy["row"]] == column) and (tp[column][rec_addy["row"]] >= begin and tp[column][rec_addy["row"]] <= end):
                                    # if value was found then add to list
                                    ret_list.append(self.table.page_ranges[pr].base_pages[bp][RID_COLUMN][base_row])
                                    break
        #if nothing matches, ret_list will be empty       
        return ret_list

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
