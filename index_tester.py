from lstore.db import Database
from lstore.query import Query
from random import choice, randint, sample, seed


db = Database()
db.open('./ECS165')

grades_table = db.create_table('Grades', 5, 0)

grades_table.index.create_index(1)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 10
number_of_updates = 100

seed(3562901)
for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished\n\n")


# Check records that were persisted in part 1
for k in range(0,20):
    record = query.select(keys[k], 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[keys[k]][i]:
            error = True
    if error:
        print('select error on', keys[k], ':', record.all_columns, ', correct:', records[keys[k]])
print("Select finished")


for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
print("Aggregate finished")

# deleted_keys = sample(keys, 100)
# for key in deleted_keys:
#     query.delete(key)
#     records.pop(key, None)

#db.close()

