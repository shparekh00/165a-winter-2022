import unittest
from lstore.query import *
from lstore.table import *
from lstore.db import *
from lstore.virtualPage import virtualPage
#https://docs.python.org/3/library/unittest.html
# delete



#run same tests on different tables: lite, m
#https://www.datacamp.com/community/tutorials/unit-testing-python
class query_test(unittest.TestCase):
    def makeSampleTable(self, name, num_columns, key):
        db = Database()
        grades_table = db.create_table(name, num_columns, key) # (name, num of cols, key)
        return grades_table

    def test_insert_one(self):
        # arrange
        tbl = self.makeSampleTable("Grades", 5, 0)
        query = Query(tbl)
        # act
        query.insert(69420, 0, 0, 0, 0)
        # assert
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[0].pages[4].read(0), 69420)


    def test_insert_multiple(self):
        # arrange
        tbl = self.makeSampleTable("Grades", 5, 0)
        query = Query(tbl)
        # act
        for i in range(0, 10000):
            query.insert(69420+i, 0, 0, 0, 0)
        # assert
        # first record of 1st page
        #10000/512 = 19.5 - 15 4.5
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[0].pages[4].read(0), 69420)
        #first record 2nd page
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[0].pages[4].read(511*8), 69420+511)
        #last record
        #@danny can you check if i did this right? should be 19th bp
        #10000/512
        self.assertAlmostEqual(tbl.page_ranges[1].base_pages[7].pages[4].read(34*8), 69420+(9999))

    def test_delete_one(self):
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

    def test_delete_multiple(self):
        # arrange
        tbl = self.makeSampleTable("Grades", 5, 0)
        query = Query(tbl)
        # act
        pr = 0 # j page ranges
        bp = 0 # k base pages
        for i in range(0, 1000):
            #to do: confirm numbers with someone and implement this correctly
            if i % 511 == 0: #idk why 511 gives error
                bp += 1
                tbl.page_ranges[pr].add_base_page()
            if bp == 15: #16 virtual pages total. 0-idxd and has 1 tp
                bp = 0
                pr += 1
                tbl.create_new_page_range()

            tmp = tbl.page_ranges[pr]
            tmp2 = tmp.base_pages[bp]
            tmp3 = tmp2.pages[4].write(i)
            tmp4 = tmp2.pages[1].write(i)
            #tbl.page_ranges[pr].base_pages[bp].pages[4].write(i)
            #tbl.page_ranges[pr].base_pages[bp].pages[1].write(i)
            tbl.page_directory[i] = {
                "page_range_id" : pr,
                "row" : i%511,
                "virtual_page_id": "B_"+str(bp)
            }
            tbl.RID_directory[i] = i

        query = Query(tbl)
        query.delete(1)
        query.delete(511)
        query.delete(9999)

        # assert
        # first record of 1st page
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[0].pages[4].read(0), 0)
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[0].pages[4].read(1), 0)
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[0].pages[0].read(1), -1)
        #first record 2nd page
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[1].pages[4].read(511), 0)
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[1].pages[4].read(511), 512)
        self.assertAlmostEqual(tbl.page_ranges[0].base_pages[1].pages[0].read(511), -1)
        #last record
        self.assertAlmostEqual(tbl.page_ranges[pr].base_pages[bp].pages[4].read(271), 0)
        self.assertAlmostEqual(tbl.page_ranges[pr].base_pages[bp].pages[0].read(271), -1)

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
        # self.assertAlmostEqual(a[0],69)
        # self.assertAlmostEqual(a[1],69420)
        print(a)
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

    def test_update_multiple(self):
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
    pass

if __name__ == '__main__':
    unittest.main()
