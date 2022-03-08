from lstore.db import Database
from lstore.query import Query
from random import choice, randint, sample, seed
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

db = Database()

grades_table = db.create_table('Grades', 5, 0)
grades_table.index.create_index(1)
query = Query(grades_table)

records = {}
keys = []
insert_transactions = []
number_of_records = 1000
number_of_aggregates = 100
seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    #skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    # print('inserted', records[key])

number_of_transactions = 1

for i in range(number_of_transactions): 
    insert_transactions.append(Transaction())

key = 6942070
records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
query.insert(*records[key])

for i in range(0, 1):
    key = 6942069 + i
    keys.append(key)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    #record_insert = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    q = Query(grades_table)
    t = insert_transactions[i % number_of_transactions]
    t.add_query(q.insert, grades_table, *records[key])
    #t.add_query(q.update, grades_table, *records[key])
    t.add_query(q.delete, grades_table, key+1)
#t.run()

transaction_worker = TransactionWorker()
transaction_worker.add_transaction(insert_transactions[0])
transaction_worker.run()
transaction_worker.join()

# Check inserted records using select query in the main thread outside workers
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record.all_columns, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")