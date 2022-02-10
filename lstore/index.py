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
        #return list of RIDs that match values
        #1. go to correct column in base
        #2. go through column to find records with the given value
        #3. check schema encoding column to see if record was modified (check for a 1)
        #3a. schema enc 0 AND we find value, note RID into list
        #3b. schema enc 1: search tp for matches. go back to bp to go through unupdated values
        #4. return list
        #print("Column", column)

        """
        given the column index and the value of that column that we are looking for
        for every page range
            for every base page
                get the matching column/page
                for the number of records/rows of the matching column/page
                    check the schema encoding
                    if the schema encoding for our column is 0 (there is no update)
                        if the value of that row is the same as the value we are looking for
                            then append the rid to our rid array
                    else if the column was changed
                        then get the tail page's rid through the indirection col
                        and then get the tail page itself from the page directory

                         if the tail page contains the updated column? and that column has the same value as the one we are lookng for
                            then get the rid of thar page and add it to our rid list
                        else
                            get through the indirection of the tail page
                            while the indirection isnt 0 (while there were still more updates)
                                get the corresponding tail page from the page directory
                                if the tail page contains the updated column and that column has the same value as the one we are lookng for
                                then get the rid of thar page and add it to our rid list
        """

        ret_list = []
        #column += 4
        for pr in range(0, len(self.table.page_ranges)):
            for bp in range(0, len(self.table.page_ranges[pr].base_pages)):
                col_page = self.table.page_ranges[pr].base_pages[bp].pages[column]
                for base_row in range(0, self.table.page_ranges[pr].base_pages[bp].pages[column].get_num_records()):
                    sch_enc = bin(self.table.page_ranges[pr].base_pages[bp].pages[SCHEMA_ENCODING_COLUMN].read(base_row * 8))[2:].zfill(self.table.num_columns - 4)
                    #print("Schema encoding: ", sch_enc, "     ", "Column: ", column, "    ", "Sch_enc[column]: ", sch_enc[column])
                    if sch_enc[column] == '0':
                        if self.table.page_ranges[pr].base_pages[bp].pages[column+4].read(base_row * 8) == value:
                            print("checkpoint 1")
                            ret_list.append(self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8))
                        continue
                    elif sch_enc[column] == '1': 
                        #print("checkpoint 2")
                        tail_rid = self.table.page_ranges[pr].base_pages[bp].pages[INDIRECTION_COLUMN].read(base_row * 8)
                        rec_addy = self.table.page_directory[tail_rid]
                        tp_id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
                        tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[tp_id]
                        tail_sch_enc = bin(tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"]))[2:].zfill(self.table.num_columns-4)
                        if tail_sch_enc[column] == '1' and (tp.pages[column+4].read(rec_addy["row"]) == value):
                            # if value was found then add to list
                            val = self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8)
                            print("checkpoint 2")
                            ret_list.append(val)
                            continue
                        # otherwise check any remaining tail pages 
                        else:   
                            #print("checkpoint 3")
                            indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])
                            # otherwise go to indirection column and check if another tail_RID exists, if so go to if and repeat check
                            while indir != 0:
                                #print("indir !=0")
                                rec_addy = self.table.page_directory[indir]
                                tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[rec_addy["virtual_page_id"]]
                                indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])
                                # check_tp_value
                                if (tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"]) == column) and (tp.pages[column+4].read(rec_addy["row"]) == value):
                                    # if value was found then add to list
                                    print("checkpoint 3")
                                    ret_list.append(self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8))
                                    break
        #if nothing matches, ret_list will be empty       
        return ret_list


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    # inclusive range
    def locate_range(self, begin, end, column): 
        ret_list = []
        
        #column += 4
        # go to each page range and repeat process until we run out of page ranges
        for pr in range(0, len(self.table.page_ranges)):
            for bp in range(0, len(self.table.page_ranges[pr].base_pages)):
                col_page = self.table.page_ranges[pr].base_pages[bp].pages[column]
                for base_row in range(0, self.table.page_ranges[pr].base_pages[bp].pages[column].get_num_records()):
                    sch_enc = bin(self.table.page_ranges[pr].base_pages[bp].pages[SCHEMA_ENCODING_COLUMN].read(base_row * 8))[2:].zfill(self.table.num_columns-4)
                    #print("Schema encoding: ", sch_enc, "     ", "Column: ", column, "    ")
                    if sch_enc[column] == '0':
                        value = self.table.page_ranges[pr].base_pages[bp].pages[column].read(base_row * 8)
                        if value >= begin and value <= end:
                            print("checkpoint 1")
                            ret_list.append(self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8))
                        continue
                    # otherwise if value doesn't match, but the rwecord was updated, check tail page for a match
                    elif sch_enc[column] == '1': 
                        #print("checkpoint 2")
                        tail_rid = self.table.page_ranges[pr].base_pages[bp].pages[INDIRECTION_COLUMN].read(base_row * 8)
                        rec_addy = self.table.page_directory[tail_rid]
                        tp_id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
                        tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[tp_id]
                        # if tail record contains updated column AND we found the value
                        tail_sch_enc = bin(tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"]))[2:].zfill(self.table.num_columns-4)
                        value = tp.pages[column].read(rec_addy["row"])
                        if tail_sch_enc[column] == '1' and value >= begin and value <= end:
                            # if value was found then add to list
                            val = self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8)
                            print("checkpoint 2")
                            ret_list.append(val)
                            continue
                        # otherwise check any remaining tail pages 
                        else:   
                            #print("checkpoint 3")
                            indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])
                            # otherwise go to indirection column and check if another tail_RID exists, if so go to if and repeat check
                            while indir != 0:
                                #print("indir !=0")
                                rec_addy = self.table.page_directory[indir]
                                tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[rec_addy["virtual_page_id"]]
                                indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])
                                # check_tp_value
                                value = tp.pages[column].read(rec_addy["row"])
                                if tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"]) == column and value >= begin and value <= end:
                                    # if value was found then add to list
                                    print("checkpoint 3")
                                    ret_list.append(self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8))
                                    #print("leaving")
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

    # LOCATE() WITH TESTING COMMENTS
    # def locate(self, column, value):
    #     #return list of RIDs that match values
    #     #1. go to correct column in base
    #     #2. go through column to find records with the given value
    #     #3. check schema encoding column to see if record was modified (check for a 1)
    #     #3a. schema enc 0 AND we find value, note RID into list
    #     #3b. schema enc 1: search tp for matches. go back to bp to go through unupdated values
    #     #4. return list
    #     #print("Column", column)

    #     """
    #     given the column index and the value of that column that we are looking for
    #     for every page range
    #         for every base page
    #             get the matching column/page
    #             for the number of records/rows of the matching column/page
    #                 check the schema encoding
    #                 if the schema encoding for our column is 0 (there is no update)
    #                     if the value of that row is the same as the value we are looking for
    #                         then append the rid to our rid array
    #                 else if the column was changed
    #                     then get the tail page's rid through the indirection col
    #                     and then get the tail page itself from the page directory

    #                      if the tail page contains the updated column? and that column has the same value as the one we are lookng for
    #                         then get the rid of thar page and add it to our rid list
    #                     else
    #                         get through the indirection of the tail page
    #                         while the indirection isnt 0 (while there were still more updates)
    #                             get the corresponding tail page from the page directory
    #                             if the tail page contains the updated column and that column has the same value as the one we are lookng for
    #                             then get the rid of thar page and add it to our rid list
    #     """

    #     ret_list = []
    #     column += 4
    #     for pr in range(0, len(self.table.page_ranges)):
    #         for bp in range(0, len(self.table.page_ranges[pr].base_pages)):
    #             col_page = self.table.page_ranges[pr].base_pages[bp].pages[column]
    #             for base_row in range(0, self.table.page_ranges[pr].base_pages[bp].pages[column].get_num_records()):
    #                 sch_enc = bin(self.table.page_ranges[pr].base_pages[bp].pages[SCHEMA_ENCODING_COLUMN].read(base_row * 8))[2:].zfill(self.table.num_columns)
    #                 #sch_enc_int = int.from_bytes(sch_enc, byteorder="big", signed=True)
    #                 # if record was not updated and value matches, add it to ret_list
    #                 #print("Binary schema encoding: ", bin(sch_enc))
    #                 #print("Binary schema encoding: ", bin(sch_enc)[column-2:])
    #                 # print("SCHEMA", sch_enc)
    #                 # print("LEn", len(sch_enc))
    #                 # print("Column", column)
    #                 if sch_enc[column] == '0':
    #                 #if sch_enc[column] == False: #CHANGED\
    #                     #print("checkpoint 1")
    #                     #print(self.table.page_ranges[pr].base_pages[bp].pages[column].read(base_row * 8))
    #                     if self.table.page_ranges[pr].base_pages[bp].pages[column].read(base_row * 8) == value:
    #                         print("checkpoint 1")
    #                         ret_list.append(self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8))
    #                     continue
    #                 # otherwise if value doesn't match, but the rwecord was updated, check tail page for a match
    #                 elif sch_enc[column] == '1': 
    #                     #print("checkpoint 2")
    #                     tail_rid = self.table.page_ranges[pr].base_pages[bp].pages[INDIRECTION_COLUMN].read(base_row * 8)
    #                     rec_addy = self.table.page_directory[tail_rid]
    #                     tp_id = self.table.page_ranges[0].get_ID_int(rec_addy["virtual_page_id"])
    #                     tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[tp_id]
    #                     # if tail record contains updated column AND we found the value
    #                     #print("Schema bin: ", bin(tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"])))
    #                     #print("Column value:", tp.pages[column].read(rec_addy["row"]), "     ", "locate value: ", value)
    #                     tail_sch_enc = bin(tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"]))[2:].zfill(self.table.num_columns)
    #                     if tail_sch_enc[column] == '1' and (tp.pages[column].read(rec_addy["row"]) == value):
    #                         # if value was found then add to list
    #                         val = self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8)
    #                         print("checkpoint 2")
    #                         ret_list.append(val)
    #                         continue
    #                     # otherwise check any remaining tail pages 
    #                     else:   
    #                         #print("checkpoint 3")
    #                         indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])
    #                         # otherwise go to indirection column and check if another tail_RID exists, if so go to if and repeat check
    #                         while indir != 0:
    #                             #print("indir !=0")
    #                             rec_addy = self.table.page_directory[indir]
    #                             tp = self.table.page_ranges[rec_addy["page_range_id"]].tail_pages[rec_addy["virtual_page_id"]]
    #                             indir = tp.pages[INDIRECTION_COLUMN].read(rec_addy["row"])
    #                             # check_tp_value
    #                             if (tp.pages[SCHEMA_ENCODING_COLUMN].read(rec_addy["row"]) == column) and (tp.pages[column].read(rec_addy["row"]) == value):
    #                                 # if value was found then add to list
    #                                 print("checkpoint 3")
    #                                 ret_list.append(self.table.page_ranges[pr].base_pages[bp].pages[RID_COLUMN].read(base_row * 8))
    #                                 #print("leaving")
    #                                 break
    
    #     #if nothing matches, ret_list will be empty       
    #     return ret_list