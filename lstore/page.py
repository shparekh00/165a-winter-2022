
from asyncio.windows_events import NULL

class Page:
    
    def __init__(self, page_id):
        self.num_records = 0
        self.data = bytearray(4096) # bytearray of size 4096, all values initialized to null
        self.page_id = page_id

    
    # return index of empty row, -1 otherwise
    def get_empty_row(self):
        if not self.has_capacity():
            return -1
            
        for i in range(0, 1023):
            if self.data[i * 4] == None:
                return i * 4
                
        return -1

    # will be deprecated in future when we redo deletes or merges to allow insertions for within the column
    # check if last row is null
    def has_capacity(self):
        return self.data[4092] == None


    # write value to row (if there is an empty space)
    def write(self, value):
        row = self.get_empty_row()
        # find null row and add value there
        if row != -1:
            self.num_records += 1
            i = 0
            for b in (value).to_bytes(4, byteorder='big'):
                self.data[row + i] = b
                i += 1
        else:
            # return error
            raise Exception("Cannot write value to page")

    #read record based on physical address (row) given
    def read(self, row):
        #TODO: check if array parsing is inclusive or exclusive
        #TODO: add input validation. if row DNE or isnt divisible by 4
        value = self.data[row:row+4]
        return value

    # RID: pageRange_basePage/tailPage_column_row   ex: 65_53_51_98
    
    # delete record from bytearray (row)
    # def delete(self, row):
    #     rid_arr = RID.split('_')
    #     # val = self.data[rid_arr[3] * 4]
    #     # delete val
    #     pass