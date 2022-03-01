from disk import Disk
from page import Page

page = Page("Grades", 0,"B_0",6)
d = Disk('./testing')

# Write
page.write(100)
file_name = d.create_file_name(page.location)
d.write_to_disk(page)

# Read
new_page = d.retrieve_from_disk(("Grades", 0,"B_0",6))
val = new_page.read(0)
print(val)

page.write(92, 0)
page.write(3, 1*8)
file_name = d.create_file_name(page.location)
d.write_to_disk(page)

new_page = d.retrieve_from_disk(("Grades", 0,"B_0",6))
val_row0 = new_page.read(0)
val_row1 = new_page.read(8)
print(val_row0)
print(val_row1)