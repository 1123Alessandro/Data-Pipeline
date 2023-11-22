import pandas as pd 
import numpy as np

df_branch_service = pd.read_json("C:/Users/reiva/Downloads/Lab Exercise 3/temp/branch_service_transaction_info.json") 
df_customer_transaction = pd.read_parquet("C:/Users/reiva/Downloads/Lab Exercise 3/temp/customer_txn_duplicates_removed.parquet")

### Lowercase last name
df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.lower()
print(df_customer_transaction.head(2))
print("Successfully Converted to Lowercase")

### Saving data
df_customer_transaction.to_parquet ("C:/Users/reiva/Downloads/Lab Exercise 3/temp/customer_txn_last_name_clean.parquet")
df_branch_service.to_parquet ("C:/Users/reiva/Downloads/Lab Exercise 3/temp/branch_service.parquet")
print(df_customer_transaction.shape)
#print(df_customer_transaction)
print("Successfully Saved the Data")