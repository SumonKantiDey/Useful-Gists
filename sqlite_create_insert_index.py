"""
EXPLAIN QUERY PLAN select * from contactjob where linkedin_suffix="b3b884105";
to check data is searched via INDEX or not
"""

import csv
import sqlite3
from collections import Counter
from itertools import islice
import ujson
from more_itertools import chunked

TOTAL_ROWS = 155745361
con = sqlite3.connect("merging.sqlite3") # will create a sqlite DB
cursor = con.cursor() #Creating a cursor object using the cursor() method
cursor.execute("DROP TABLE IF EXISTS contactjob") #Doping EMPLOYEE table if already exists.


#Creating table as per requirement
sql ='''CREATE TABLE contactjob(
   Id integer primary key autoincrement,
   leadbook_id      TEXT    NOT NULL,
   linkedin_suffix  TEXT     NOT NULL
);'''
cursor.execute(sql)
print("Table created successfully........")

# Creating an unique index (CREATE INDEX contactjob_linkedin_suffix ON contactjob (linkedin_suffix))
table_name = "contactjob"
index_leadbook_id = "contactjob_leadbook_id"
leadbook_id_column = "leadbook_id"
index_linkedin_suffix = "contactjob_linkedin_suffix"
linkedin_suffix_column = "linkedin_suffix"
cursor.execute('CREATE INDEX {ix} on {tn}({cn})'.format(ix=index_leadbook_id, tn=table_name, cn=leadbook_id_column))
cursor.execute('CREATE INDEX {ix} on {tn}({cn})'.format(ix=index_linkedin_suffix, tn=table_name, cn=linkedin_suffix_column))


def load_csv(filename):
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader, None)
        print(reader)
        for leadbook_id, linkedin_suffix in reader:
            yield leadbook_id, linkedin_suffix

def save_to_db(filename, table_name):
    for batch in chunked(load_csv(filename), n=3):
        values = ','.join(str(tuple(r)) for r in batch)
        print(values)
        cursor.execute(f"INSERT INTO {table_name}(leadbook_id, linkedin_suffix) VALUES {values}")

    # Commit your changes in the database
    con.commit()
    #Closing the connection
    con.close()

if __name__ == "__main__":
    save_to_db(
        filename = "sample_data.csv",
        table_name="contactjob"
    )