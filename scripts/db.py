# py ./python/db.py
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
engine = create_engine('postgresql+psycopg2://postgres:pass@localhost/test')

# conn_string = 'postgresql://postgres:dbm4ster@localhost/test'
conn_string = "host='localhost' dbname='test' user='postgres' password='pass'"
conn = psycopg2.connect(conn_string)

conn.autocommit = True
cursor = conn.cursor()

table = pd.read_parquet('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/Transaction Table.parquet')

print(table.shape)

data_2 = table[["txn_id", "avail_date", "last_name", "first_name", "birthday"]]
# Create DataFrame
print(data_2)

# converting data to sql
table.to_sql('transaction', engine, if_exists='replace', index=False)

# fetching all rows
sql2 = '''select * from transaction;'''
print(f'sql2: {sql2}')
cursor.execute(sql2)
for i in cursor.fetchall():
    print(i)

