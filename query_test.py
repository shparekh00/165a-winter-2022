import unittest
from lstore.query import *
from lstore.table import *
from lstore.db import *
#https://docs.python.org/3/library/unittest.html
# delete



#run same tests on different tables: lite, m
#https://www.datacamp.com/community/tutorials/unit-testing-python
class query_test(unittest.TestCase):
    def makeSampleTable(self, name, num_columns, key):
        db = Database()
        grades_table = db.create_table(name, num_columns, key) # (name, num of cols, key)
        query = Query(grades_table)
        return grades_table

    def test_insert_easy(self):
        # arrange
        tbl = self.makeSampleTable("Grades", 5, 0)
        query = Query(tbl)
        # act
        query.insert(69420, 0, 0, 0, 0)
        # assert
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[0].pages[4].read(0), 69420)
                               #(69240).to_bytes(8, byteorder='big', signed=True))

    def test_delete_easy(self):
        tbl = self.makeSampleTable("Grades", 5, 0)
        # emulate inserting x records
        # tbl.page_ranges[0].base_page[0].pages[4].data[0:8] = 69420
        
        #call delete
        
        #run self.assertAlmostEqual()

    #test inserting when record dne
    #test inserting when bp is full
    #test inserting 1 record
    #test inserting 1000 records
    #test inserting 10000 records
    #test inserting when pr is full

    #test deleting a record that dne
    #test deleting 1000 records
    #test deleting 10000 records
    #test deleting when table dne



    #test selecting with 0 matches
    #test selecting match with same column being returned
    #test selecting match with multiple columns returned

    #test updating when record dne
    #test updating 1 record
    #test updating 1000 records
    #test updating 10000 records
    #test updating when pr is full
    #test updating when tp is full

    #test sum
    pass

if __name__ == '__main__':
    unittest.main()
# # insert
# class test_insert(unittest.TestCase):
#     pass
# # select
# class test_select(unittest.TestCase):
#     pass
# # update
# class test_update(unittest.TestCase):
#     pass
# # sum
# class test_sum(unittest.TestCase):
#     pass
# do we have a problem???  eyes yew roe ll wemdojoi (like outback we steak bitch)no vegans