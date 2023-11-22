import pandas as pd 
from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://postgres:dbm4ster@localhost/test')
import psycopg2 

#conn_string = 'postgresql://postgres:dbm4ster@localhost/test'
conn_string = "host='localhost' dbname='test' user='postgres' password='dbm4ster'"
conn = psycopg2.connect(conn_string)

conn1 = psycopg2.connect(
  database="test", 
  user='postgres',  
  password='dbm4ster',  
  host='localhost',  
  port= '5432'
) 

conn.autocommit = True
cursor = conn.cursor() 

df_1 = pd.read_parquet("C:/Users/reiva/Downloads/Lab Exercise 3/temp/customer_txn_last_name_clean.parquet")
df_2 = pd.read_parquet("C:/Users/reiva/Downloads/Lab Exercise 3/temp/branch_service.parquet")

print(df_1.shape)

data_2 = df_1[["txn_id","avail_date","last_name","first_name","birthday"]] 
# Create DataFrame 
print(data_2) 
  
# converting data to sql 
data_2.to_sql('cus_info_transaction', engine, if_exists='replace', index=False) 
  
# fetching all rows 
sql2='''select * from cus_info_transaction;'''
cursor.execute(sql2) 
for i in cursor.fetchall(): 
    print(i) 

#data_1 = df_2[["txn_id","branch","service","price"]] 
# Create DataFrame 
#print(data_1) 
  
# converting data to sql 
#data_1.to_sql('branch_service_transaction', conn, if_exists= 'replace') 
  
# fetching all rows 
#sql1='''select * from branch_service_transaction;'''
#cursor.execute(sql1) 
#for i in cursor.fetchall(): 
#    print(i) 

conn1.commit() 
conn1.close() 