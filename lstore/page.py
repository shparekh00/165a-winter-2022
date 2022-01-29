
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096) # bytearray of size 4096, all values initialized to null

    def has_capacity(self):
        # check if theres any null values in self.data??
        # or maybe check count of records?
        pass

    def write(self, value):
        self.num_records += 1
        pass

