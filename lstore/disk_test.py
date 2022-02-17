from disk import Disk
from page import Page

page = Page("Students", 1,"B_1",1)
page.write(100)
d = Disk('./testing')
file_name = d.create_file_name(page)
d.write_to_file(page, file_name)