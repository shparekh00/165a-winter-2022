from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore.virtualPage import virtualPage
from lstore.basePage import basePage
from lstore.tailPage import tailPage
from lstore.pageRange import PageRange
import os
import json

BUFFER_POOL_SIZE = 100

class Database():

    def __init__(self):
        self.tables = {} 
        self.path = "./ECS165"
        self.bufferpool = Bufferpool(self.path)
        pass

    def open(self, path):
        if path:
            self.path = path

        self.bufferpool.set_path(self.path)
        self.bufferpool.path = self.path

        # TODO: Create all the tables that already exist
        # TODO: Populate those tables with the page dir, rid dir
        # TODO: Create page ranges for all tables, and create its virtual pages and fill page IDs in virtual pages
        
        # open table directory file
        file_name = path + "/table_directory.json"

        if os.path.exists(file_name):
            file = open(file_name,)
            tables_file = json.load(file)

            # For every table in tables_file
            for name in tables_file:
                # Parse the table for the num_columns, key, and num_page_ranges
                table = tables_file[name]
                num_columns = table["num_columns"]
                key = table["key"]
                num_page_ranges = table["num_page_ranges"]
                RID_counter = table["RID_counter"]

                # Add the table to memory
                self.add_table_from_disk(name, num_columns, key, num_page_ranges, RID_counter)

                # Get the page_directory
                file_name = path + "/" + name + "_page_directory.json"
                if os.path.exists(file_name):
                    file = open(file_name,)
                    page_directory = json.load(file)
                    page_directory = { int(key):val for key,val in page_directory.items() }
                    self.tables[name].page_directory = page_directory

                # Get the rid directory
                file_name = path + "/" + name + "_rid_directory.json"
                if os.path.exists(file_name):
                    file = open(file_name,)
                    rid_directory = json.load(file)
                    rid_directory = { int(key):int(val) for key,val in rid_directory.items() }
                    self.tables[name].rid_directory = rid_directory

                # Get the log
                # TODO: Will val work? Can val be a Record object or does it need to be an array? Etc.
                # TODO: hi shivani -alvin
                file_name = path + "/" + name + "_log_directory.json"
                if os.path.exists(file_name):
                    file = open(file_name,)
                    log_directory = json.load(file)
                    log_directory = { int(key):val for key,val in log_directory.items() }
                    self.tables[name].log = log_directory

                # Add indices
                # index_file = open(self.path + "/" + name + "_index_directory_" + str(i) + ".json", "w")
                for i in range(0, num_columns+5):
                    file_name = self.path + "/" + name + "_index_directory_" + str(i) + ".json"
                    if os.path.exists(file_name):
                        file = open(file_name,)
                        index_directory = json.load(file)

                        # https://stackoverflow.com/questions/21193682/convert-a-string-key-to-int-in-a-dictionary
                        index_directory = { int(key):val for key,val in index_directory.items() }
                        #print(index_directory)
                        self.tables[name].index.indices[i] = index_directory

        # Initialize the bufferpool
        #self.bufferpool.path = path

        pass

    """
    Function that creates all tables from disk and stores the page ID's/page_locations into our virtual pages.
    :param num_columns, key, num_page_ranges: the table_directory values for a specificw table

        # Algorithm:
        # For every page range that was in the table
        # We need to determine how many base pages this page range contained, and how many tail pages this page range contained
        # Since every page range can have varying amounts of base and tail pages,
        # let's index through the possible base and tail pages starting from B_0/T_0 until we can't find an existing file in the disk
        # EXAMPLE: 
        #     Go from B_0 -> B_10. We find that B_10 doesn't exist. So now we go to tail pages from T_0. We iterate through, and 
        #     we find that T_5 doesn't exist. That means this page range is now
        #     base_pages = [virtualPage, virtualPage, virtualPage, virtualPage, virtualPage, virtualPage, virtualPage, virtualPage, virtualPage, virtualPage]
        #     tail_pages = [virtualPage, virtualPage, virtualPage, virtualPage, virtualPage,]

        # File Structures
        # Page in disk: Students-0-B_1-2.txt
        # Page_location tuple: (table_name, pr_id, virtual_page_id, page_id)

    # TODO: hello shivani we need to test this -alvin
    """
    def add_table_from_disk(self, name, num_columns, key, num_page_ranges, RID_counter):
        # Creating table
        existing_table = True
        self.create_table(name, num_columns, key, existing_table)

        table = self.tables[name]

        table.RID_counter = RID_counter
        # Lol
        path = self.path

        for page_range_index in range(0, num_page_ranges):
            # We don't need to add a page_range if it's the first page range.
            # if page_range_index != 0:
            #     table.page_ranges.append(PageRange(name, page_range_index, num_columns+5))
            #     table.page_range_id += 1

            table.page_ranges.append(PageRange(name, page_range_index, num_columns+5))
            table.page_range_id += 1
            
            page_range = table.page_ranges[page_range_index]

            # Variables to help us get the associated file
            bp_index = 0
            bp_index_str = str(bp_index)
            file_name = path + "/" + name + "-" + str(page_range_index) + "-B_" + bp_index_str + "-0.txt"

            while os.path.exists(file_name):
                # Create new base page; since a PageRange is initialized with one BP and one TP, if bp_index is 0, we don't need to add
                if bp_index != 0:
                    page_range.increment_basepage_id()
                    page_range.base_pages.append(basePage(name, page_range_index, "B_" + bp_index_str, num_columns+5))

                bp_pages = page_range.base_pages[-1].pages
                for col in range(0, num_columns+5):
                    bp_pages.append((name, page_range_index, "B_" + bp_index_str, col))

                # Update our variables so we can parse for the next file
                bp_index += 1
                bp_index_str = str(bp_index)
                file_name = path + "/" + name + "-" + str(page_range_index) + "-B_" + bp_index_str + "-0.txt"

        
            # Repeating the same functionality for tail pages
            tp_index = 0
            tp_index_str = str(tp_index)
            file_name = path + "/" + name + "-" + str(page_range_index) + "-T_" + tp_index_str + "-0.txt"
            
            while os.path.exists(file_name):
                # Create new tail page; since a PageRange is initialized with one BP and one TP, if bp_index is 0, we don't need to add
                if tp_index != 0:
                    page_range.increment_tailpage_id()
                    page_range.tail_pages.append(tailPage(name, page_range_index, "T_" + tp_index_str, num_columns+5))

                tp_pages = page_range.tail_pages[-1].pages
                for col in range(0, num_columns+5):
                    tp_pages.append((name, page_range_index, "T_" + tp_index_str, col))                    

                tp_index += 1
                tp_index_str = str(tp_index)
                file_name = path + "/" + name + "-" + str(page_range_index) + "-T_" + tp_index_str + "-0.txt"

            # Done: we now have all the base and tail pages for this page range
        pass

    def close(self):
    
        # Write everything that's dirty in the Bufferpool to Disk
        self.bufferpool.write_all_to_disk()

        # create new dictionary for tables - contains num columns and key
        table_directory = {}
        # Add page directory and RID directory to disk
        for name, table in self.tables.items():
            page_dir_file_name = name + "_page_directory.json"
            rid_dir_file_name = name + "_rid_directory.json"
            log_dir_file_name = name + "_log_directory.json"

            # Page directory file write
            page_dir_file = open(self.path + "/" + page_dir_file_name, "w")
            json.dump(table.page_directory, page_dir_file)
            page_dir_file.close()

            # RID directory file write
            rid_dir_file = open(self.path + "/" + rid_dir_file_name, "w")
            json.dump(table.RID_directory, rid_dir_file)
            rid_dir_file.close()

            table_directory[name] = {"num_columns": table.num_columns, "key": table.key, "num_page_ranges": len(table.page_ranges), "RID_counter": table.RID_counter}

            # Log directory write
            log_dir_file = open(self.path + "/" + log_dir_file_name, "w")
            json.dump(table.log, log_dir_file)
            log_dir_file.close()
            
            # Index write
            i = 0
            for index_dict in table.index.indices:
                if index_dict != None:
                    index_file = open(self.path + "/" + name + "_index_directory_" + str(i) + ".json", "w")
                    json.dump(index_dict, index_file)
                    index_file.close()
                i += 1

        # Table directory file write
        table_dir_file = open(self.path + "/table_directory.json", "w")
        json.dump(table_directory, table_dir_file)
        table_dir_file.close()

        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index, existing_table=False):
        # table gets passed a bufferpool
        table = Table(name, num_columns, key_index, self.bufferpool)
        if (name not in self.tables):
            self.tables[name] = table # store table in database tables list

        if not existing_table:
            table.create_new_page_range()
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        del self.tables[name] # remove table from dictionary
        pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
