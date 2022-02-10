import unittest
from query import *
from table import *
from db import *
#https://docs.python.org/3/library/unittest.html
# delete
def makeSampleTableLite(self, name):
    db = Database()
    grades_table = db.create_table(name, 5, 0)
    query = Query(grades_table)
    return grades_table

#run same tests on different tables: lite, m
class test_query(unittest.TestCase):
    def test_delete_easy(self):
        tbl = makeSampleTableLite("Grades")
        #emulate inserting x records


        #call delete
        #run self.assertAlmostEqual()
        pass
    #test deleting a record that dne
    #test deleting 1000 records
    #test deleting 10000 records
    #test deleting when table dne

    #test inserting when record dne
    #test inserting when bp is full
    #test inserting 1 record
    #test inserting 1000 records
    #test inserting 10000 records
    #test inserting when pr is full

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