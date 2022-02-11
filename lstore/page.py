
class Page:
    
    def __init__(self, page_id):
        self.num_records = 0
        self.data = bytearray(4096) # bytearray of size 4096, all values initialized to 0
        self.page_id = page_id  # represents column in virtual page? might not be necessary

    
    # return index of empty row, -1 otherwise
    def get_empty_row(self):
        if not self.has_capacity():
            return -1
            
        for i in range(0, 512):
            if self.data[i * 8] == 0:
                return i * 8
                
        return -1

    # return number of records
    def get_num_records(self):
        return self.num_records

    # will be deprecated in future when we redo deletes or merges to allow insertions for within the column
    # check if last row is null
    def has_capacity(self):
        return self.num_records < 512

    # used to update schema encoding and indirection of base pages
    def update(self, value, row):
            for i, b in enumerate((value).to_bytes(8, byteorder='big', signed=True)):
                self.data[row + i] = b

    # write value to row (if there is an empty space)
    def write(self, value, row = None):
        if row == None:
            row = self.get_empty_row()

        # find null row and add value there
        if row != -1:
            self.num_records += 1
            for i, b in enumerate((value).to_bytes(8, byteorder='big', signed=True)):
                self.data[row + i] = b
        else:
            print("Cannot write to page")
            raise Exception("Cannot write value to page")

    #read record based on physical address (row) given
    def read(self, row):
        #TODO: add input validation. if row DNE or isnt divisible by 8
        value = self.data[row:row+8]
        return int.from_bytes(value, 'big', signed=True)

    # delete record from bytearray (row)
    def delete(self, row):
        if(row>512 or row < 0):
            return False
        self.write(0, row)
        pass