from disk import Disk
from page import Page

page = Page("Students", 1,"B_2",1)
d = Disk('./testing')

# Write
page.write(100)
file_name = d.create_file_name(page.location)
d.write_to_disk(page, file_name)

# Read
new_page = d.retrieve_from_disk(("Students", 1, "B_2", 1))
val = new_page.read(0)
print(val)

page.write(92, 0)
page.write(3, 1*8)
file_name = d.create_file_name(page.location)
d.write_to_disk(page, file_name)

new_page = d.retrieve_from_disk(("Students", 1, "B_2", 1))
val = new_page.read(0)
print(val)