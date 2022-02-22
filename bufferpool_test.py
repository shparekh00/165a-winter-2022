from lstore.db import Database
from random import randint


db = Database()
db.open('./ECS165A')
grades_table = db.create_table('Grades', 5, 0)

grades_table.create_new_page_range()

# get page location info for one page
# grades_table.page_ranges[-1].base_pages[-1].pages[0].write(100)
# page_location = grades_table.page_ranges[-1].base_pages[-1].pages[0].location 
#print("location: ", page_location)

#print("Before access: ", grades_table.bufferpool.pin_counts)
page_location = grades_table.page_ranges[-1].base_pages[-1].pages[0]
accessed_page = grades_table.access_page_from_memory(page_location)
#print("Test that correct page is accessed")
#print("Row 0: ", accessed_page.read(0))

#print("After access: ", grades_table.bufferpool.pin_counts)

grades_table.finish_page_access(page_location)

#print("After finish: ",grades_table.bufferpool.pin_counts)

for i in range(0, 20):
    grades_table.create_new_page_range()

# print("Testing disk files")
#grades_table.page_ranges[1].tail_pages[-1].pages[0].write(5)
page_location = grades_table.page_ranges[1].tail_pages[-1].pages[0] 
accessed_page = grades_table.access_page_from_memory(page_location)
# print("After access: ", grades_table.bufferpool.pin_counts)
grades_table.finish_page_access(page_location)
# print("After finish: ",grades_table.bufferpool.pin_counts)

"""
for i in range(1,40):
    pr = randint(1,21)
    pos = randint(0,8)
    page_location = grades_table.page_ranges[pr].tail_pages[-1].pages[pos].location 
    print("Iteration ", i)
    accessed_page = grades_table.access_page_from_memory(page_location)
"""
db.close()
db.open('./ECS165A')
db.close()
