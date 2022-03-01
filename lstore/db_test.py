from db import Database
from page import Page

from random import choice, randint, sample, seed

db = Database()
db.open('./testing123')
grades_table = db.create_table('Grades', 5, 0)
page = Page("Grades", 0,"B_0",6)
d = grades_table.bufferpool.disk

# Write
page.write(100, 0)
print("reading..", page.read(0))
d.write_to_disk(page)
temp = d.retrieve_from_disk(("Grades", 0,"B_0",6))
print("temp is: ", temp.read(0))

# Close
db.close()

print("Open 2")


db.open('./testing123')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

new_page = grades_table.bufferpool.disk.retrieve_from_disk(("Grades", 0,"B_0",6))
val = new_page.read(0)
print("Value is: ", val)
db.close()
