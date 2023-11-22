import pandas as pd
import numpy as np

df_branch_service = pd.read_json("C:/Users/reiva/Downloads/Lab Exercise 3/temp/branch_service_transaction_info.json") 
df_customer_transaction = pd.read_parquet ("C:/Users/reiva/Downloads/Lab Exercise 3/temp/customer_txn_last_name_clean.parquet") 
print("Successfully Loaded the Data")

### Saving merged data
df_merged = pd.merge (df_customer_transaction, df_branch_service)
df_merged.to_parquet ("merged_data.parquet")
print("Successfully Saved Merged Data")

### Saving yearly view
df_merged ['avail_date'] = pd.to_datetime (df_merged ['avail_date'])
df_merged.groupby([df_merged. avail_date.dt.year, 'branch_name']) ['price'].sum().to_frame() 
df_merged.to_parquet ("C:/Users/reiva/Downloads/Lab Exercise 3/temp/yearly_view.parquet")
print("Successfully Saved Yearly Branch View")