from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

import csv

outputText = ""

for j in range(0,3):
    print("Iteration ", j+1)
    # Student Id and 4 grades
    db = Database()
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)
    keys = []

    header = ['insert', 'update', 'select', 'sum', 'delete']
    
    insert_time_0 = process_time()
    for i in range(0, 7000):
        query.insert(906659671 + i, 93, 0, 0, 0)
        keys.append(906659671 + i)
    insert_time_1 = process_time()
    print("Inserting 7000 records took:  \t\t\t", insert_time_1 - insert_time_0)
    result = insert_time_1 - insert_time_0
    outputText += str(result) + ", "

    # Measuring update Performance
    update_cols = [
        [None, None, None, None, None],
        [None, randrange(0, 100), None, None, None],
        [None, None, randrange(0, 100), None, None],
        [None, None, None, randrange(0, 100), None],
        [None, None, None, None, randrange(0, 100)],
    ]

    update_time_0 = process_time()
    for i in range(0, 7000):
        query.update(choice(keys), *(choice(update_cols)))
    update_time_1 = process_time()
    print("Updating 7000 records took:  \t\t\t", update_time_1 - update_time_0)
    result = update_time_1 - update_time_0
    outputText += str(result) + ", "

    # Measuring Select Performance
    select_time_0 = process_time()
    for i in range(0, 7000):
        query.select(choice(keys),0 , [1, 1, 1, 1, 1])
    select_time_1 = process_time()
    print("Selecting 7000 records took:  \t\t\t", select_time_1 - select_time_0)
    result = select_time_1 - select_time_0
    outputText += str(result) + ", "

    # Measuring Aggregate Performance
    agg_time_0 = process_time()
    for i in range(0, 7000, 100):
        start_value = 906659671 + i
        end_value = start_value + 100
        result = query.sum(start_value, end_value - 1, randrange(0, 5))
    agg_time_1 = process_time()
    print("Aggregate 7000 of 100 record batch took:\t", agg_time_1 - agg_time_0)
    result = agg_time_1 - agg_time_0
    outputText += str(result) + ", "

    # Measuring Delete Performance
    delete_time_0 = process_time()
    for i in range(0, 7000):
        query.delete(906659671 + i)
    delete_time_1 = process_time()
    print("Deleting 7000 records took:  \t\t\t", delete_time_1 - delete_time_0)
    result = delete_time_1 - delete_time_0
    outputText += str(result) + " "
    outputText += "\n"

# output to file 
outputFile = open("output7000.csv", "w")
# writer = csv.writer(outputFile)
# writer.writerow(header)
# writer.writerow(outputText)
outputFile.write("insert, update, select, sum, delete\n")
outputFile.write(outputText)
outputFile.close()