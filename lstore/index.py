"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

INDIRECTION_COLUMN = 0
RID_COLUMN = 1 
TIMESTAMP_COLUMN = 2 
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        #self.indices = [None] *  (self.table.num_columns+5)
        
        pass

    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):
        ret_list = []
        # iterate through all page ranges, then all base pages, then all records
        for pr in range(0, len(self.table.page_ranges)):
            for bp in range(0, len(self.table.page_ranges[pr].base_pages)):
                for base_row in range(0, self.table.page_ranges[pr].base_pages[bp].pages[column+5].get_num_records()):
                    sch_enc = bin(self.table.page_ranges[pr].base_pages[bp].pages[SCHEMA_ENCODING_COLUMN].read(base_row * 8))[2:].zfill(self.table.num_columns)

                    # If value was not updated, add value from base page record
                    if sch_enc[column] == '0':
                        if self.table.page_ranges[pr].base_pages[bp].pages[column+5].read(base_row * 8) == value:
                            ret_list.append(self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8))
                        continue

                    # Otherwise, check tail pages for most recent update to that column
                    elif sch_enc[column] == '1': 
                        tail_rid = self.table.page_ranges[pr].base_pages[bp].pages[INDIRECTION_COLUMN].read(base_row * 8)
                        rec_addy = self.table.page_directory[tail_rid]
                        tp_id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
                        tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[tp_id]
                        tail_sch_enc = bin(tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"]))[2:].zfill(self.table.num_columns)

                        # if value was found then add to list
                        if tail_sch_enc[column] == '1' and (tp.pages[column+5].read(rec_addy["row"]) == value):
                            val = self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8)
                            ret_list.append(val)
                            continue

                        # otherwise check any remaining tail pages 
                        else:   
                            indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])
                            # Go to indirection column and check if another tail_RID exists, if it does, go to indir_RID and repeat
                            while indir != 0:
                                rec_addy = self.table.page_directory[indir]
                                tp_id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
                                tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[tp_id]
                                indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])

                                # if value was found then add to list
                                if (tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"]) == column) and (tp.pages[column+5].read(rec_addy["row"]) == value):
                                    ret_list.append(self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8))
                                    break
        return ret_list


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column): 
        ret_list = []
        primary_key_index = 5

        # go to each page range and repeat process until we run out of page ranges
        for pr in range(0, len(self.table.page_ranges)):
            for bp in range(0, len(self.table.page_ranges[pr].base_pages)):
                for base_row in range(0, self.table.page_ranges[pr].base_pages[bp].pages[column+5].get_num_records()):
                    primary_key = self.table.page_ranges[pr].base_pages[bp].pages[primary_key_index].read(base_row * 8)
                    sch_enc = bin(self.table.page_ranges[pr].base_pages[bp].pages[SCHEMA_ENCODING_COLUMN].read(base_row * 8))[2:].zfill(self.table.num_columns)

                    # If base page has primary key in range and the column we're interested in was not updated, then grab its value
                    if primary_key >= begin and primary_key <= end:
                        if sch_enc[column] == '0':
                            ret_list.append(self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8))
                            continue

                        # Otherwise if the column was updated, check tail page for most recent update
                        elif sch_enc[column] == '1': 
                            tail_rid = self.table.page_ranges[pr].base_pages[bp].pages[INDIRECTION_COLUMN].read(base_row * 8)
                            rec_addy = self.table.page_directory[tail_rid]
                            tp_id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
                            tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[tp_id]

                            tail_sch_enc = bin(tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"]))[2:].zfill(self.table.num_columns)
                            primary_key = tp.pages[primary_key_index].read(rec_addy["row"])

                            # ASSUMPTION: primary key cannot be updated
                            # if most recent update to column is found, then add the base page's RID to the list
                            if tail_sch_enc[column] == '1':
                                val = self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8)
                                ret_list.append(val)
                                continue

                            # otherwise check any remaining tail pages 
                            else:   
                                indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])

                                # otherwise go to indirection column and check if another tail_RID exists
                                while indir != 0:
                                    rec_addy = self.table.page_directory[indir]
                                    tp_id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
                                    tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[tp_id]
                                    indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])
                                    
                                    primary_key = tp.pages[primary_key_index].read(rec_addy["row"])
                                    tail_sch_enc = bin(tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"]))[2:].zfill(self.table.num_columns)
                                    
                                    # if most recent update to column is found, then add the base page's RID to the list
                                    if tail_sch_enc[column] == '1':
                                        ret_list.append(self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8))
                                        break
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