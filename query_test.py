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
        return grades_table

    def test_insert_easy(self):
        # arrange
        tbl = self.makeSampleTable("Grades", 5, 0)
        query = Query(tbl)
        # act
        query.insert(69420, 0, 0, 0, 0)
        # assert
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[0].pages[4].read(0), 69420)


    def test_delete_easy(self):
        # arrange
        tbl = self.makeSampleTable("Grades", 5, 0)
        # act: inserting manually and delete
        tbl.page_ranges[0].base_pages[0].pages[4].write(69420)
        tbl.page_ranges[0].base_pages[0].pages[1].write(69)
        tbl.page_directory[69] = {
            "page_range_id" : 0,
            "row" : 0,
            "virtual_page_id": "B_0"
        }
        tbl.RID_directory[69420] = 69
        
        query = Query(tbl)
        query.delete(69420)
        #print("Indirection: ", tbl.page_ranges[0].base_pages[0].pages[0].read(0))
        # assert
        # row data is 0
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[0].pages[4].read(0), 0)
        # indir is -1
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[0].pages[0].read(0), -1)

    def test_select_easy(self):
        #def select(self, index_value, index_column, query_columns):
        # arrange
        tbl = self.makeSampleTable("Grades", 5, 0)
        # act: inserting x records
        ## write RID
        tbl.page_ranges[0].base_pages[0].pages[1].write(69)
        ## insert into random cols
        tbl.page_ranges[0].base_pages[0].pages[4].write(69420)
        tbl.page_ranges[0].base_pages[0].pages[5].write(69)
        tbl.page_ranges[0].base_pages[0].pages[6].write(420)
        tbl.page_ranges[0].base_pages[0].pages[7].write(69)
        tbl.page_ranges[0].base_pages[0].pages[8].write(420)
        tbl.page_directory[69] = {
            "page_range_id" : 0,
            "row" : 0,
            "virtual_page_id": "B_0"
        }
        tbl.RID_directory[69420] = 69
        
        query = Query(tbl)
        a = query.select(69420, 4, [1, 1, 1, 1, 1])
        #print(a)
        # assert
        #self.assertAlmostEqual(a,{})
        #record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
#         error = False
#         for j, column in enumerate(record.columns):
#             if column != records[key][j]:
#                 error = True
#         if error:
#             print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
#         else:
#             pass
#             # print('update on', original, 'and', updated_columns, ':', record)
#         updated_columns[i] = None
    def test_update_easy(self):
        #def select(self, index_value, index_column, query_columns):
        # arrange
        tbl = self.makeSampleTable("Grades", 5, 0)
        # act: inserting x records
        ## write RID
        tbl.page_ranges[0].base_pages[0].pages[1].write(69, 0)
        ## insert into random cols
        tbl.page_ranges[0].base_pages[0].pages[4].write(69420, 0)
        tbl.page_ranges[0].base_pages[0].pages[5].write(69, 0)
        tbl.page_ranges[0].base_pages[0].pages[6].write(420, 0)
        tbl.page_ranges[0].base_pages[0].pages[7].write(69, 0)
        tbl.page_ranges[0].base_pages[0].pages[8].write(420, 0)
        tbl.page_directory[69] = {
            "page_range_id" : 0,
            "row" : 0,
            "virtual_page_id": "B_0"
        }
        tbl.RID_directory[69420] = 69
        query = Query(tbl)
        # query.update(choice(keys), *(choice(update_cols)))
        query.update(69420, None,None,1,None,None)
        self.assertAlmostEqual(tbl.page_ranges[0].tail_pages[0].pages[6].read(0), 1)
    
    def test_sum_easy(self):
        #def select(self, index_value, index_column, query_columns):
        # arrange
        tbl = self.makeSampleTable("Grades", 5, 0)
        # act: inserting x records
        ## write RID
        for i in range(0,2):
            tbl.page_ranges[0].base_pages[0].pages[1].write(69+i, i*8)
            ## insert into random cols
            tbl.page_ranges[0].base_pages[0].pages[4].write(10+i, i*8)
            tbl.page_ranges[0].base_pages[0].pages[5].write(1, i*8)
            tbl.page_ranges[0].base_pages[0].pages[6].write(1, i*8)
            tbl.page_ranges[0].base_pages[0].pages[7].write(1, i*8)
            tbl.page_ranges[0].base_pages[0].pages[8].write(1, i*8)
        tbl.page_directory[69] = {
            "page_range_id" : 0,
            "row" : 0,
            "virtual_page_id": "B_0"
        }
        tbl.page_directory[70] = {
            "page_range_id" : 0,
            "row" : 8,
            "virtual_page_id": "B_0"
        }
        tbl.RID_directory[10] = 69
        tbl.RID_directory[11] = 70
        query = Query(tbl)
        self.assertAlmostEqual(query.sum(10, 11, 2), 2)
#         column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
#         result = query.sum(keys[r[0]], keys[r[1]], c)
#         if column_sum != result:
#             print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
#         else:
#             pass
#             # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)

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