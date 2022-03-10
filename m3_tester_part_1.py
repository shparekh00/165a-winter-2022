from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# creating grades table
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 100
number_of_transactions = 100
num_threads = 1

# create index on the non primary columns
try:
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

keys = []
records = {}
seed(3562901)

# array of insert transactions
insert_transactions = []

for i in range(number_of_transactions):
    insert_transactions.append(Transaction())

for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    t = insert_transactions[i % number_of_transactions]
    t.add_query(query.insert, grades_table, *records[key])

transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker())

for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(insert_transactions[i])



# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()


# Check inserted records using select query in the main thread outside workers
insertCount = 0
errorCount = 0
emptyCount = 0
workCount = 0

for key in keys:
    result = query.select(key, 0, [1, 1, 1, 1, 1])
    if result != False and len(result) > 0:
        insertCount += 1
        record = result[0]
        error = False
        for i, column in enumerate(record.columns):
            if column != records[key][i]:
                error = True
        if error:
            errorCount += 1
            print('select error on', key, ':', record.columns, ', correct:', records[key])
        else:
            workCount += 1
            pass
    else: 
        emptyCount += 1
            # print('select on', key, ':', record)
print("Select finished")
# KEEP THIS FOR GRAPHS LATER (FOR PRESENTATION)
# print("Percentage of correct inserts: ", (insertCount/len(keys)))
# print("Percentage of incorrect inserts: ", (emptyCount/len(keys)))
# print("Percentage of correct selects out of correct inserts: ", (workCount/insertCount))
# print("Percentage of incorrect selects out of correct inserts: ", (errorCount/insertCount))

# Increased number of records = increased percentage of errors among selected records that were successfully inserted,
#print("RID Directory part 1: ", grades_table.RID_directory)
db.close()