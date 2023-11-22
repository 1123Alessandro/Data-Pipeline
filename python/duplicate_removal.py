import pandas as pd
import numpy as np

df_customer_transaction = pd.read_json("C:/Users/reiva/Downloads/Lab Exercise 3/temp/customer_transaction_info.json")
print("Succesfully Loaded the Data")

### Dropping duplicates
print("Dropping Duplicates")
print(df_customer_transaction.shape)
df_customer_transaction = df_customer_transaction.drop_duplicates()
print(df_customer_transaction.shape)

### Saving data
df_customer_transaction.to_parquet("C:/Users/reiva/Downloads/Lab Exercise 3/temp/customer_txn_duplicates_removed.parquet")
print("Successfully Saved the Data")