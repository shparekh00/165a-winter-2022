from lstore.db import Database
from lstore.query import Query
from random import choice, randint, sample, seed


db = Database()
db.open('./ECS165')

grades_table = db.create_table('Grades', 5, 0)

grades_table.index.create_index(1)

query = Query(grades_table)

key = 92106429
record = [key, 3, 0, 7, 14]
query.insert(*record)

record_select = query.select(key, 0, [1, 1, 1, 1, 1])[0]
for i, col in enumerate(record_select.columns):
    if col != record[i]:
        print("Select error")
###############
updated_columns = [None, None, 1, None, None]
query.update(key, *updated_columns)

record[2] = 1
record_update = query.select(key, 0, [1, 1, 1, 1, 1])[0]

for i, col in enumerate(record_update.columns):
    if col != record[i]:
        print(col, i)
        print("update error 2")
###############  
updated_columns = [None, None, None, 3, None]
query.update(key, *updated_columns)

record[3] = 3
record_update = query.select(key, 0, [1, 1, 1, 1, 1])[0]

for i, col in enumerate(record_update.columns):
    if col != record[i]:
        print("update error 1")
###############
