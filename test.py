import pandas as pd
import pyperclip as pc

bs = pd.read_json('./branch_service_transaction_info.json')
ct = pd.read_json('./customer_transaction_info.json')

print(ct['txn_id'].shape)
print(ct['txn_id'].nunique())

for i in ct.columns:
    x = ct[i]
    print(f'{i}: {x.dtype}')
    print(f'{x.shape[0]} elements with {x.nunique()} unique elements')
    print(x.unique())
    print('--------------------------------------------------')
    print()

for i in bs.columns:
    x = bs[i]
    print(f'{i}: {x.dtype}')
    print(f'{x.shape[0]} elements with {x.nunique()} unique elements')
    print(x.unique())
    print('--------------------------------------------------')
    print()

bs['price'].dtype

bs.query('price > 99')

